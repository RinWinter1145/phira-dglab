#!/usr/bin/env python3
"""
DG-LAB 郊狼「模拟 APP」端到端测试。

通过 adb 截取 Phira 屏幕上的配对二维码 → 解码出 ws 地址 + clientId →
用纯标准库 WebSocket 客户端模拟官方 DG-LAB APP 的配对流程：
  1. 连接 ws 服务器
  2. 接收服务器下发的 bind 消息（拿到自己作为 APP 端的 id）
  3. 回发 bind(clientId=控制端id, targetId=自己的id)
  4. 收到 bind message:"200" 视为配对成功
  5. 之后持续接收并打印服务器转发来的强度消息（strength-X+2+Y）

用法:
  python3 dglab_app_sim.py            # 一次截屏+配对+观察
  python3 dglab_app_sim.py --watch    # 配对后持续观察强度消息（Ctrl+C 退出）
"""

import argparse
import base64
import hashlib
import json
import os
import re
import socket
import struct
import subprocess
import sys
import time

import cv2

ADB = ["adb"]

# ---------------------------------------------------------------------------
# 截屏 + 二维码解码
# ---------------------------------------------------------------------------

def screenshot_path():
    return "/tmp/dglab_screen.png"


def capture_and_decode():
    """截屏并返回二维码内容字符串，找不到返回 None。"""
    remote = "/sdcard/dglab_screen.png"
    subprocess.run(ADB + ["shell", "screencap", "-p", remote],
                   check=True, capture_output=True)
    subprocess.run(ADB + ["pull", remote, screenshot_path()],
                   check=True, capture_output=True)
    img = cv2.imread(screenshot_path())
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    h, w = gray.shape

    # 优先用 zbar（对小模块/抗锯齿二维码更稳），cv2 兜底。
    try:
        import zbar
        zimg = zbar.Image(w, h, "Y800", gray.tobytes())
        scanner = zbar.ImageScanner()
        scanner.parse_config("enable")
        scanner.scan(zimg)
        for s in scanner.results:
            data = s.data if isinstance(s.data, str) else s.data.decode()
            if data and "DGLAB-SOCKET" in data:
                return data.strip()
    except ImportError:
        pass

    # cv2 兜底（含放大、二值化增强）
    det = cv2.QRCodeDetector()
    for scale in (1, 2, 3):
        im = gray if scale == 1 else cv2.resize(gray, (w * scale, h * scale),
                                                interpolation=cv2.INTER_CUBIC)
        data, _, _ = det.detectAndDecode(im)
        if data and "DGLAB-SOCKET" in data:
            return data.strip()
    return None


def parse_qr(content):
    """
    解析二维码内容：
      https://www.dungeon-lab.com/app-download.php#DGLAB-SOCKET#ws://ip:port/clientId
    返回 (ws_url, client_id)。不符格式抛 ValueError。
    """
    parts = content.split("#")
    if len(parts) != 3 or parts[1] != "DGLAB-SOCKET":
        raise ValueError(f"二维码格式不符: {content!r}")
    tail = parts[2]
    # tail 形如 ws://ip:port/clientId（最后一个 / 是分隔）
    if "://" not in tail:
        raise ValueError(f"二维码缺少协议: {tail!r}")
    scheme, rest = tail.split("://", 1)
    if "/" not in rest:
        raise ValueError(f"二维码缺少 clientId: {tail!r}")
    hostport, client_id = rest.rsplit("/", 1)
    ws_url = f"{scheme}://{hostport}"
    return ws_url, client_id


# ---------------------------------------------------------------------------
# 极简 WebSocket 客户端（标准库实现，仅支持 ws:// 明文、无分片大数据场景）
# ---------------------------------------------------------------------------

class WsClient:
    def __init__(self, url):
        self.url = url
        self.sock = None
        # 解析
        m = re.match(r"ws://([^/:]+)(?::(\d+))?(/.*)?$", url)
        if not m:
            raise ValueError(f"仅支持 ws:// 地址，got {url}")
        self.host = m.group(1)
        self.port = int(m.group(2)) if m.group(2) else 80
        self.path = m.group(3) or "/"

    def set_blocking(self):
        """进入观察阶段前，把 socket 设为无限等待（避免无消息时 10s 超时爆 TimeoutError）。"""
        if self.sock is not None:
            self.sock.settimeout(None)

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=10)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        self.sock.sendall(req.encode())
        # 读响应头
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = self.sock.recv(1024)
            if not chunk:
                raise ConnectionError("握手时连接关闭")
            buf += chunk
        head, _, rest = buf.partition(b"\r\n\r\n")
        status = head.split(b"\r\n", 1)[0]
        if b"101" not in status:
            raise ConnectionError(f"握手失败: {head.decode(errors='replace')}")
        self._recv_buf = bytearray(rest)
        return self

    def _recv_exact(self, n):
        while len(self._recv_buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("连接已断开")
            self._recv_buf.extend(chunk)
        out = bytes(self._recv_buf[:n])
        del self._recv_buf[:n]
        return out

    def recv_text(self):
        """阻塞读取下一条文本消息。"""
        while True:
            h = self._recv_exact(2)
            b0, b1 = h[0], h[1]
            fin = (b0 & 0x80) != 0
            opcode = b0 & 0x0F
            masked = (b1 & 0x80) != 0
            length = b1 & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._recv_exact(8))[0]
            mask = self._recv_exact(4) if masked else None
            payload = self._recv_exact(length)
            if masked and mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode == 0x8:  # close
                raise ConnectionError("对端关闭连接")
            if opcode == 0x9:  # ping -> pong
                self._send_frame(0xA, payload)
                continue
            if opcode == 0x1 and fin:  # text
                return payload.decode("utf-8", errors="replace")
            # 其他帧先忽略（本场景只有小文本帧）

    def _send_frame(self, opcode, payload: bytes):
        b0 = 0x80 | opcode
        n = len(payload)
        mask = os.urandom(4)
        if n < 126:
            header = struct.pack(">BB", b0, 0x80 | n)
        elif n < 65536:
            header = struct.pack(">BBH", b0, 0x80 | 126, n)
        else:
            header = struct.pack(">BBQ", b0, 0x80 | 127, n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def send_text(self, text: str):
        self._send_frame(0x1, text.encode("utf-8"))

    def close(self):
        try:
            self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 模拟 APP 配对流程
# ---------------------------------------------------------------------------

def simulate_app(ws_url, qr_client_id, watch=False):
    print(f"[*] 连接 {ws_url} ...")
    ws = WsClient(ws_url).connect()
    print(f"[*] 已连接，等待服务器下发本端 id ...")

    my_id = None

    # 第一步：等服务器给自己分配 id
    while my_id is None:
        msg = json.loads(ws.recv_text())
        if msg.get("type") == "bind" and not msg.get("targetId"):
            my_id = msg.get("clientId")
            print(f"[+] 服务器分配本端 id: {my_id}")
            break

    # 第二步：发起配对 bind(clientId=控制端id, targetId=自己的id)
    print(f"[*] 发起配对 bind({qr_client_id}, {my_id}) ...")
    ws.send_text(json.dumps({
        "type": "bind",
        "clientId": qr_client_id,
        "targetId": my_id,
        "message": "bind",
    }))

    # 第三步：等配对结果
    paired = False
    deadline = time.time() + 10
    while time.time() < deadline:
        msg = json.loads(ws.recv_text())
        if msg.get("type") == "bind" and msg.get("message") == "200":
            paired = True
            print(f"[+] 配对成功 (client={msg.get('clientId')} target={msg.get('targetId')})")
            break
        elif msg.get("type") == "bind":
            print(f"[-] 配对失败: {msg}")
            break
    if not paired:
        print("[-] 未在 10s 内配对成功")
        ws.close()
        return False

    # 配对成功：观察强度消息
    if watch:
        # 观察阶段不设超时，没打歌时安静等待，打歌后实时打印。
        ws.set_blocking()
        print("gunmu")
        try:
            while True:
                msg = json.loads(ws.recv_text())
                t = msg.get("type")
                m = msg.get("message", "")
                if m.startswith("strength") or t == "msg":
                    print(f" 收到: {json.dumps(msg, ensure_ascii=False)}")
                elif t == "heartbeat":
                    print(f" 心跳 {m}")
                else:
                    print(f" {json.dumps(msg, ensure_ascii=False)}")
        except KeyboardInterrupt:
            pass
    ws.close()
    return True


def main():
    ap = argparse.ArgumentParser(description="DG-LAB 模拟 APP 测试端")
    ap.add_argument("--watch", action="store_true", help="配对后持续观察强度消息")
    ap.add_argument("--qr", help="直接给定二维码内容（跳过截屏）")
    args = ap.parse_args()

    # 尝试多次截屏解码（开关刚开时二维码可能出现有延迟）
    content = args.qr
    if not content:
        for i in range(10):
            content = capture_and_decode()
            if content:
                break
            print(f"[.] 第 {i+1} 次截屏未找到二维码，1s 后重试...")
            time.sleep(1)
    if not content:
        print("[-] 无法从屏幕解析出二维码（请确认郊狼开关已开、二维码已显示）")
        sys.exit(1)

    print(f"[+] 二维码内容:\n    {content}")
    ws_url, client_id = parse_qr(content)
    print(f"[+] 解析 → ws_url={ws_url!r}  clientId={client_id!r}")

    ok = simulate_app(ws_url, client_id, watch=args.watch)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
