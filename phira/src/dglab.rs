//! DG-LAB (郊狼 Coyote) 控制集成。
//!
//! 传输层抽象：WebSocket（v2 协议）为当前主要实现；BLE 传输可后续在同一 trait 后新增。
//! 协议细节见 `docs/dglab-ws-protocol.md`（官方仓库 dungeonlab-open/dglab-websocket-simple 的 v2 文档）。

use anyhow::{bail, Context, Result};
use arc_swap::ArcSwap;
use futures_util::{SinkExt, StreamExt};
use qrcode::{EcLevel, QrCode};
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, warn};

use once_cell::sync::Lazy;

/// 把用户填写的地址规范化为可连的 WebSocket URL：
/// `127.0.0.1:1145` → `ws://127.0.0.1:1145`；带 scheme 的（ws://、wss://）原样保留。
pub fn normalize_ws_url(raw: &str) -> String {
    let s = raw.trim();
    if s.contains("://") {
        s.to_owned()
    } else {
        format!("ws://{s}")
    }
}

/// 探测本机在局域网中的 IPv4 地址。
///
/// 通过向一个公网地址发起「不发送任何数据」的 UDP connect 来查询系统路由选出的本地出口 IP。
/// 这是无需任何额外权限的常见技巧（Android 上同样可用）。
/// 失败时回退到 `127.0.0.1`。
pub fn detect_lan_ip() -> String {
    // 8.8.8.8:80 仅用于触发路由表查询，不会真正发包。
    let sock = UdpSocket::bind("0.0.0.0:0").ok();
    if let Some(sock) = sock {
        if sock.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = sock.local_addr() {
                let ip = addr.ip();
                if ip.is_ipv4() && !ip.is_loopback() {
                    return ip.to_string();
                }
            }
        }
    }
    "127.0.0.1".to_owned()
}

/// 连接会话的共享状态（供 UI 读取配对信息）。
#[derive(Default)]
pub struct DglabState {
    /// 连接地址（规范化后）
    pub url: String,
    /// 服务端分配的第三方终端 ID（出现后可生成配对二维码）
    pub client_id: String,
    /// 是否已与 APP 完成配对（收到 bind 200）
    pub paired: bool,
}

/// 生成配对二维码纹理（黑底白模块，模块像素数可调）。
pub fn qr_texture(content: &str, module_px: u32) -> prpr::ext::SafeTexture {
    let code = QrCode::with_error_correction_level(content.as_bytes(), EcLevel::M).expect("QR too large");
    let modules = code.to_colors();
    let size = code.width() as u32 * module_px;
    let mut buf = vec![255u8; (size * size * 4) as usize];
    for y in 0..code.width() as u32 {
        for x in 0..code.width() as u32 {
            let dark = modules[(y * code.width() as u32 + x) as usize] == qrcode::Color::Dark;
            for dy in 0..module_px {
                for dx in 0..module_px {
                    let i = (((y * module_px + dy) * size + (x * module_px + dx)) * 4) as usize;
                    if dark {
                        buf[i..i + 4].copy_from_slice(&[0, 0, 0, 255]);
                    }
                }
            }
        }
    }
    macroquad::prelude::Texture2D::from_rgba8(size as u16, size as u16, &buf).into()
}

/// 转发到设备的判定类型（映射至配置的三档强度）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DglabKind {
    Perfect,
    Good,
    BadMiss,
}

impl DglabKind {
    /// 将判定结果映射为郊狼输出的档位。
    pub fn from_judgement(judgement: prpr::judge::Judgement) -> Self {
        use prpr::judge::Judgement::*;
        match judgement {
            Perfect => DglabKind::Perfect,
            Good => DglabKind::Good,
            Bad | Miss => DglabKind::BadMiss,
        }
    }
}

/// 传输层契约；BLE 传输后续实现同一 trait 即可无缝替换。
pub trait DglabTransport: Send {
    /// 当前是否已连接（尽力而为的指示）。
    fn connected(&self) -> bool;
    /// 设置强度（范围 0..=200，两通道同值；后续可扩展为单通道/波形）。
    fn set_strength(&self, strength: u8);
    fn disconnect(&self);
}

// =============================================================================
// 内嵌 WS 服务器（局域网直连模式）
//
// Phira 自己扮演官网 v2 SOCKET 协议里的「WebSocket 服务端」：
//   - 监听 TCP 端口，接受手机 APP 的连接
//   - 每个连接分配 clientId/targetId
//   - 处理 APP 发来的 bind，建立「控制端终端 ↔ APP」的配对
//   - 把控制端的强度指令转成 `strength-通道+2+强度` 用 type:"msg" 发给已配对的 APP
//
// 二维码指向 Phira 本机 IP，手机 APP 扫码后在同一局域网内直连本服务器。
// =============================================================================

/// 服务器内部维护的单个连接元信息。
struct ClientConn {
    router: mpsc::UnboundedSender<WsMessage>,
    /// 是否为 APP 端（由 bind 或消息特征判定；默认按“控制端”处理）。
    is_app: bool,
}

/// 内嵌 DG-LAB v2 SOCKET 服务器。
pub struct DglabServer {
    /// 服务器对外可见的地址（用于生成二维码），例如 `ws://192.168.1.5:9999`。
    pub url: String,
    /// 本机控制端的终端 ID（clientId）。
    client_id: String,
    /// 已配对 APP 的 targetId（空表示未配对）。
    target_id: Mutex<String>,
    /// 连接表：clientId -> 连接。
    conns: Mutex<HashMap<String, ClientConn>>,
    /// 供 UI 轮询的共享状态（client_id / paired / url）。
    pub shared: Arc<Mutex<DglabState>>,
    /// 监听是否仍在运行（用于 connected()）。
    running: AtomicBool,
    /// 关闭信号：触发后 accept_loop 退出并释放监听端口。
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl DglabServer {
    /// 绑定端口并启动服务器。`listen_host` 为监听地址（如 `0.0.0.0`），
    /// `public_host` 为对外可见地址（本机局域网 IP，用于二维码）。
    pub fn bind(listen_host: &str, public_host: &str, port: u16, client_id: String) -> Result<Arc<Self>> {
        let bind_addr = format!("{listen_host}:{port}");
        let url = format!("ws://{public_host}:{port}");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = Arc::new(Self {
            url,
            client_id,
            target_id: Mutex::new(String::new()),
            conns: Mutex::new(HashMap::new()),
            shared: Arc::new(Mutex::new(DglabState::default())),
            running: AtomicBool::new(false),
            shutdown_tx,
        });
        server.shared.lock().unwrap().url = server.url.clone();
        server.shared.lock().unwrap().client_id = server.client_id.clone();

        let listener = std::net::TcpListener::bind(&bind_addr).with_context(|| format!("无法在 {bind_addr} 上监听"))?;
        listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(listener)?;
        let arc = Arc::clone(&server);
        tokio::spawn(async move { arc.accept_loop(listener, shutdown_rx).await });
        Ok(server)
    }

    /// 停止监听（触发 accept_loop 退出、释放端口）。幂等。
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
        let _ = self.shutdown_tx.send(true);
    }

    /// 根据配置解析出 `(监听 host, 对外 host, 端口)`。
    /// 手动填写的地址优先；留空则监听 `0.0.0.0`、对外用探测到的本机 IP、端口用默认。
    pub fn resolve_addr(manual: &str, default_port: u16) -> (String, String, u16) {
        let manual = manual.trim();
        if manual.is_empty() {
            let ip = detect_lan_ip();
            return ("0.0.0.0".to_owned(), ip, default_port);
        }
        // 手动地址可能形如 `ip:port` / `ws://ip:port` / 纯 `ip`。
        let raw = match manual.split_once("://") {
            Some((_, rest)) => rest,
            None => manual,
        };
        let (host, port) = match raw.rsplit_once(':') {
            Some((h, p)) if p.parse::<u16>().is_ok() => (h.to_owned(), p.parse::<u16>().unwrap()),
            _ => (raw.to_owned(), default_port),
        };
        ("0.0.0.0".to_owned(), host, port)
    }

    async fn accept_loop(self: Arc<Self>, listener: TcpListener, mut shutdown_rx: tokio::sync::watch::Receiver<bool>) {
        self.running.store(true, Ordering::Relaxed);
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    // 收到关闭信号：退出循环，listener drop 释放端口。
                    self.running.store(false, Ordering::Relaxed);
                    break;
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _addr)) => {
                            let arc = Arc::clone(&self);
                            tokio::spawn(async move { arc.handle_conn(stream).await });
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle_conn(self: Arc<Self>, stream: TcpStream) {
        let ws = match tokio_tungstenite::accept_async(stream).await {
            Ok(ws) => ws,
            Err(e) => {
                debug!("dglab server: ws accept failed: {e}");
                return;
            }
        };
        let (mut sink, mut src) = ws.split();
        let (router_tx, mut router_rx) = mpsc::unbounded_channel::<WsMessage>();

        // 为该连接分配 clientId（上游对每个连接分配 uuid；这里用自增计数保证唯一即可）。
        let conn_id = self.alloc_id();
        {
            let mut conns = self.conns.lock().unwrap();
            conns.insert(conn_id.clone(), ClientConn { router: router_tx.clone(), is_app: false });
        }

        // 回发 bind 告知 clientId（与控制端登录服务器时一样）。
        let mut paired_target: Option<String> = None;
        self.send_bind(&conn_id, "", "targetId", &mut sink).await;

        loop {
            tokio::select! {
                out = router_rx.recv() => {
                    let Some(msg) = out else { break };
                    if sink.send(msg).await.is_err() { break; }
                }
                incoming = src.next() => {
                    match incoming {
                        Some(Ok(WsMessage::Text(text))) => {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                if self.on_message(&conn_id, &v, &mut sink, &mut paired_target).await {
                                    // on_message 返回 true 表示需要退出该连接
                                    break;
                                }
                            }
                        }
                        Some(Ok(WsMessage::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(_)) => break,
                    }
                }
            }
        }

        // 清理连接与配对。
        self.cleanup(&conn_id);
    }

    fn alloc_id(&self) -> String {
        use std::sync::atomic::AtomicU64;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("c{}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    async fn send_bind(
        &self,
        client_id: &str,
        target_id: &str,
        message: &str,
        sink: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, WsMessage>,
    ) {
        let msg = serde_json::json!({"type":"bind","clientId":client_id,"targetId":target_id,"message":message});
        let _ = sink.send(WsMessage::Text(msg.to_string())).await;
    }

    /// 处理一条来自已连接客户端的消息。返回 true 表示应断开该连接。
    async fn on_message(
        &self,
        conn_id: &str,
        v: &serde_json::Value,
        sink: &mut futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<TcpStream>, WsMessage>,
        paired_target: &mut Option<String>,
    ) -> bool {
        let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
        let msg_ci = v.get("clientId").and_then(|x| x.as_str()).unwrap_or("");
        let msg_ti = v.get("targetId").and_then(|x| x.as_str()).unwrap_or("");

        match ty {
            "bind" => {
                // APP 扫码后发起 bind：clientId=控制端终端ID, targetId=APP 自己的 ID。
                // 这里 self.client_id 是控制端终端；若 msg_ci == self.client_id 表示 APP 想绑定控制端。
                let is_app_bind = msg_ci == self.client_id || !msg_ti.is_empty();
                if is_app_bind {
                    let tid = if msg_ti.is_empty() { conn_id.to_owned() } else { msg_ti.to_owned() };
                    // 记录该 APP 连接为 app 端，并建立配对。
                    {
                        let mut conns = self.conns.lock().unwrap();
                        if let Some(c) = conns.get_mut(conn_id) {
                            c.is_app = true;
                        }
                    }
                    *self.target_id.lock().unwrap() = tid.clone();
                    *paired_target = Some(tid.clone());
                    self.shared.lock().unwrap().paired = true;
                    warn!("dglab server: paired with APP target={tid}");
                    self.send_bind(&self.client_id, &tid, "200", sink).await;
                }
            }
            "heartbeat" => {
                let msg = serde_json::json!({"type":"heartbeat","clientId":self.client_id,"targetId":"","message":"200"});
                let _ = sink.send(WsMessage::Text(msg.to_string())).await;
            }
            "msg" | "break" | "error" => {
                // APP 回传的强度/反馈/断开等，服务器侧基本无需处理；按官方逻辑透传/忽略。
                if ty == "break" {
                    self.shared.lock().unwrap().paired = false;
                    return true;
                }
            }
            _ => {
                debug!("dglab server recv: {v}");
            }
        }
        false
    }

    fn cleanup(&self, conn_id: &str) {
        let mut conns = self.conns.lock().unwrap();
        conns.remove(conn_id);
        let mut tid = self.target_id.lock().unwrap();
        if !tid.is_empty() {
            // 若断开的是已配对的 APP，清除配对状态。
            let still_app = conns.values().any(|c| c.is_app);
            if !still_app {
                tid.clear();
                self.shared.lock().unwrap().paired = false;
            }
        }
    }
}

impl DglabTransport for DglabServer {
    fn connected(&self) -> bool {
        !self.target_id.lock().unwrap().is_empty()
    }

    fn set_strength(&self, strength: u8) {
        let tid = match self.target_id.lock().unwrap().clone() {
            t if t.is_empty() => return,
            t => t,
        };
        // 官方 v2：type:"msg" + message:"strength-通道+2+强度"，两通道同值。
        for channel in 1..=2i32 {
            let message = format!("strength-{channel}+2+{strength}");
            if let Some(conn) = self.conns.lock().unwrap().get(&tid) {
                let _ = conn.router.send(WsMessage::Text(
                    serde_json::json!({"type":"msg","clientId":self.client_id,"targetId":tid,"message":message}).to_string(),
                ));
            }
        }
    }

    fn disconnect(&self) {
        self.target_id.lock().unwrap().clear();
        self.shared.lock().unwrap().paired = false;
    }
}

/// 全局单例服务器（供设置页与游戏内共享，保证配对与脉冲指向同一实例）。
/// 用 `ArcSwap` 以便开启/关闭开关时动态挂载与释放（释放时停止监听端口）。
pub static SERVER: Lazy<ArcSwap<Option<Arc<DglabServer>>>> = Lazy::new(|| ArcSwap::from_pointee(None));

/// 挂载全局服务器单例。传入 `None` 时同时关闭旧服务器（释放监听端口）。
pub fn set_server(server: Option<Arc<DglabServer>>) -> Option<Arc<DglabServer>> {
    let prev = SERVER.swap(Arc::new(server));
    match &*prev {
        Some(s) => {
            // 替换/卸载旧服务器时主动关闭它，释放端口。
            s.shutdown();
            Some(Arc::clone(s))
        }
        None => None,
    }
}

/// 读取当前全局服务器（若已挂载）。
pub fn current_server() -> Option<Arc<DglabServer>> {
    SERVER.load().as_ref().clone()
}

enum WsCommand {
    Strength(u8),
}

/// WebSocket 传输实现（DG-LAB v2 SOCKET 协议）。
pub struct WsTransport {
    tx: mpsc::Sender<WsCommand>,
    state: Arc<AtomicBool>,
    /// 供 UI 读取配对/地址信息的共享状态。
    pub shared: Arc<Mutex<DglabState>>,
    #[allow(dead_code)] // 保留任务句柄以控制生命周期（脱离/退出时 abort）
    handle: tokio::task::JoinHandle<()>,
}

impl WsTransport {
    /// 建立到 v2 中继服务的连接，并在后台任务中管理会话与重连。
    /// 连接成功后可通过 `shared` 读取 clientId 生成配对二维码。
    pub fn connect(url: impl Into<String>) -> Self {
        let url = normalize_ws_url(&url.into());
        let (tx, rx) = mpsc::channel(64);
        let state = Arc::new(AtomicBool::new(false));
        let shared = Arc::new(Mutex::new(DglabState {
            url: url.clone(),
            ..DglabState::default()
        }));
        let handle = tokio::spawn(Self::run(url, rx, state.clone(), Arc::clone(&shared)));
        Self {
            tx,
            state,
            shared,
            handle,
        }
    }

    async fn run(url: String, mut rx: mpsc::Receiver<WsCommand>, state: Arc<AtomicBool>, shared: Arc<Mutex<DglabState>>) {
        loop {
            let result = Self::session(&url, &mut rx, &state, &shared).await;
            warn!("dglab: session ended: {result:?} - reconnecting in 3s");
            state.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn session(
        url: &str,
        rx: &mut mpsc::Receiver<WsCommand>,
        state: &AtomicBool,
        shared: &Mutex<DglabState>,
    ) -> Result<()> {
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.context("dglab ws connect failed")?;
        state.store(true, Ordering::Relaxed);

        // 初始绑定：分配/确认 clientId（服务端可能回发 bind 消息告知）
        ws.send(WsMessage::Text(serde_json::json!({"type":"bind","clientId":"pending","targetId":"","message":"bind"}).to_string()))
            .await?;

        let mut client_id = String::new();
        let mut target_id = String::new();

        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(WsCommand::Strength(strength)) => {
                        for channel in 1..=2i32 {
                            let msg = serde_json::json!({
                                "type": 3,
                                "channel": channel,
                                "strength": strength,
                                "clientId": client_id,
                                "targetId": target_id,
                                "message": "set channel",
                            });
                            ws.send(WsMessage::Text(msg.to_string())).await?;
                        }
                    }
                    None => return Ok(()),
                },
                next = ws.next() => match next {
                    Some(Ok(WsMessage::Text(text))) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                            if let Some(ty) = v.get("type").and_then(serde_json::Value::as_str) {
                                match ty {
                                    "bind" => {
                                        if let Some(cid) = v.get("clientId").and_then(serde_json::Value::as_str) {
                                            if client_id.is_empty() {
                                                client_id = cid.to_owned();
                                                let qr = format!("https://www.dungeon-lab.com/app-download.php#DGLAB-SOCKET#{url}/{cid}");
                                                warn!("dglab: clientId={cid}");
                                                warn!("dglab: 扫码配对（或 APP 连接服务器后扫描）: {qr}");
                                            }
                                            // 同步共享状态供 UI 生成二维码
                                            if shared.lock().unwrap().client_id.is_empty() {
                                                shared.lock().unwrap().client_id = cid.to_owned();
                                            }
                                        }
                                        if let Some(tid) = v.get("targetId").and_then(serde_json::Value::as_str) {
                                            if !tid.is_empty() {
                                                target_id = tid.to_owned();
                                            }
                                        }
                                        if v.get("message").and_then(serde_json::Value::as_str) == Some("200") {
                                            shared.lock().unwrap().paired = true;
                                            warn!("dglab: paired (client={client_id} target={target_id})");
                                        }
                                    }
                                    "break" => {
                                        // 对端断开/配对解散：清空配对状态并退出会话，触发外层重连重新注册
                                        shared.lock().unwrap().paired = false;
                                        warn!("dglab: received break (opposite disconnected) - reconnecting");
                                        bail!("dglab: opposite side disconnected");
                                    }
                                    "heartbeat" => {
                                        let msg = serde_json::json!({"type":"heartbeat","clientId":client_id,"targetId":target_id,"message":"200"});
                                        ws.send(WsMessage::Text(msg.to_string())).await?;
                                    }
                                    _ => debug!("dglab recv: {text}"),
                                }
                            } else {
                                debug!("dglab recv: {text}");
                            }
                        } else {
                            debug!("dglab recv: {text}");
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(err.into()),
                    None => bail!("dglab ws closed"),
                },
            }
        }
    }
}

impl DglabTransport for WsTransport {
    fn connected(&self) -> bool {
        self.state.load(Ordering::Relaxed)
    }

    fn set_strength(&self, strength: u8) {
        let _ = self.tx.try_send(WsCommand::Strength(strength));
    }

    #[allow(dead_code)] // 传输契约的一部分，BLE 后端接入后亦使用
    fn disconnect(&self) {
        self.state.store(false, Ordering::Relaxed);
    }
}

/// 从配置读取强度值（0..=200，非法/空值默认 0）。
impl Drop for WsTransport {
    fn drop(&mut self) {
        self.disconnect();
        self.handle.abort();
    }
}

fn read_configured_strength(raw: &str, fallback: u8) -> u8 {
    raw.trim().parse::<i32>().ok().map_or(fallback, |v| v.clamp(0, 200) as u8)
}

/// 测试与 WebSocket 中继服务的连通性（握手成功即返回，随即断开）。
/// 用于设置页开启连接时的一次性测试。
pub async fn test_ws_connection(url: &str) -> Result<()> {
    let url = normalize_ws_url(url);
    tokio::time::timeout(Duration::from_secs(5), async move {
        let (_ws, _resp) = tokio_tungstenite::connect_async(&url).await.context("无法连接 WebSocket 服务器")?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("连接超时（5 秒内未完成握手）"))?
}

/// 统一的传输句柄：内嵌服务器 或 客户端（连外部中继）。
enum Transport {
    Server(Arc<DglabServer>),
    Client(WsTransport),
}

impl DglabTransport for Transport {
    fn connected(&self) -> bool {
        match self {
            Transport::Server(s) => s.connected(),
            Transport::Client(c) => c.connected(),
        }
    }

    fn set_strength(&self, strength: u8) {
        match self {
            Transport::Server(s) => s.set_strength(strength),
            Transport::Client(c) => c.set_strength(strength),
        }
    }

    fn disconnect(&self) {
        match self {
            Transport::Server(s) => s.disconnect(),
            Transport::Client(c) => c.disconnect(),
        }
    }
}

/// 将 DGLAB 传输接入游戏判定事件流。
/// 包装已有的 `UpdateFn`（若存在），在其基础上每帧把新增判定转译为强度指令。
/// 未启用时原样返回，零开销。
pub fn build_dglab_update_fn(mut inner: Option<UpdateFn>) -> Option<UpdateFn> {
    let cfg = crate::get_data().config.clone();
    if !cfg.dglab_enabled {
        return inner;
    }
    // 优先复用设置页已绑定的内嵌服务器（局域网直连），否则退化为连外部中继客户端。
    let transport: Transport = if let Some(server) = current_server() {
        Transport::Server(server)
    } else {
        Transport::Client(WsTransport::connect(cfg.dglab_ws_url.clone()))
    };
    Some(Box::new(move |time, res, judge| {
        pump_judgements(&transport, judge, &cfg);
        if let Some(f) = inner.as_mut() {
            f(time, res, judge)
        }
    }))
}

use prpr::scene::UpdateFn;

/// 将判定事件队列转译为强度指令并发送到传输层。
/// 在 GameScene 的每帧 `UpdateFn` 中调用（judgements 已被 Judge 填充）。
pub fn pump_judgements(transport: &dyn DglabTransport, judge: &prpr::judge::Judge, config: &prpr::config::Config) {
    let mut drained = judge.judgements.borrow_mut().drain(..).collect::<Vec<_>>();
    if drained.is_empty() {
        return;
    }
    for (t, _line_id, _note_id, result) in drained.drain(..) {
        let kind = match result {
            Ok(j) => DglabKind::from_judgement(j),
            Err(perfect) => {
                if perfect {
                    DglabKind::Perfect
                } else {
                    DglabKind::Good
                }
            }
        };
        let strength = match kind {
            DglabKind::Perfect => read_configured_strength(&config.dglab_perfect_power, 80),
            DglabKind::Good => read_configured_strength(&config.dglab_good_power, 50),
            DglabKind::BadMiss => read_configured_strength(&config.dglab_badmiss_power, 100),
        };
        if transport.connected() {
            transport.set_strength(strength);
        } else {
            debug!(t, ?kind, strength, "dglab: not connected, drop pulse");
        }
    }
}