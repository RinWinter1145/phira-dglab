# DG-LAB（郊狼）WebSocket 控制协议参考

> 来源：官方开源仓库 `dungeonlab-open/dglab-websocket-simple` 的 `socket/v2/README.md`（当前版本 `20260730_v0` 时抓取整理）。
> 官方站点：https://opendoc.dglab.cn （本机抓取时返回 403，请自行访问或走代理）。
> 官方 SDK：https://github.com/dungeonlab-open/dglab-kit （面向 DG-LAB 4 APP 的 TypeScript SDK，含 V4/V3 协议参考）。
> 官方 WS 服务端参考实现：https://github.com/dungeonlab-open/dglab-websocket-server
> 网页蓝牙直连测试：https://www.dungeon-lab.com/bluetooth.html

## 概述

SOCKET 控制功能：DG-LAB APP 通过 WebSocket 服务连接到外部第三方控制端，控制端通过 SOCKET 向 APP 发送数据指令，使郊狼进行脉冲输出。开发者可通过网页、游戏、脚本或其他终端在局域网或公网环境对郊狼进行控制。

- 该功能仅支持**郊狼脉冲主机 3.0**（V2 协议）。
- 拓扑：`N 个 APP 终端 ⇄ SOCKET 服务 ⇄ N 个第三方终端`，支持多人同时控制。

## 快速开始（官方参考实现）

1. 启动后端（Node.js）：`cd socket/v2/backend && npm install && npm start`（默认端口 9999）
2. 打开前端 `socket/v2/frontend/index.html`，修改 `wsConnection.js` 里的 WebSocket 地址为实际服务器地址
3. DG-LAB APP → SOCKET 功能 → 点击连接服务器 → 扫描前端页面二维码完成配对

后端环境变量：`PORT`(9999)、`HEARTBEAT_INTERVAL`(60000ms)、`DEFAULT_PUNISHMENT_TIME`(1)、`DEFAULT_PUNISHMENT_DURATION`(5)、`LOG_LEVEL`(info)

## 消息格式总则（V2）

所有消息均为 JSON：`{ "type", "clientId", "targetId", "message" }`

| 字段 | 说明 |
|---|---|
| type | 消息类型（见下） |
| clientId | 第三方终端 ID（网页端） |
| targetId | APP 端 ID（初始连接时可为空） |

约束：JSON 字符最大长度 1950（超出 APP 丢弃）；除初始连接外四个字段 value 均不可为空；ID 推荐 UUID v4。

## 连接配对流程

1. 前端连接 WS 服务 → 服务端分配 clientId
2. 前端生成二维码：`https://www.dungeon-lab.com/app-download.php#DGLAB-SOCKET#ws://服务器地址:端口/终端ID`（恰好两个 `#`，无多余路径）
3. APP 扫码连接 → 服务端分配 targetId
4. APP 发送 bind 请求 → 服务端建立配对 → 双方收到 `message:"200"` 绑定成功

## 前端 → 服务端消息

### 强度减少 (type: 1)

```json
{ "type": 1, "channel": 1, "message": "set channel", "clientId": "xxx", "targetId": "xxx" }
```
channel: 1=A 通道, 2=B 通道 → 服务端转义为 `strength-通道+0+1`（减少 1）

### 强度增加 (type: 2)

同上（+1 增加 1）

### 强度设置到指定值 (type: 3) ★ 本文档集成使用的主指令

```json
{ "type": 3, "channel": 2, "strength": 35, "message": "set channel", "clientId": "xxx", "targetId": "xxx" }
```
strength 范围 0~200 → 服务端转义为 `strength-通道+2+目标值`

### 直接转发 APP 指令 (type: 4)

```json
{ "type": 4, "message": "clear-1", "clientId": "xxx", "targetId": "xxx" }
```

### 发送波形 (type: "clientMsg")

```json
{ "type": "clientMsg", "channel": "A", "time": 5, "message": "A:[\"0A0A0A0A64646464\",...]", "clientId": "xxx", "targetId": "xxx" }
```
服务端加 `pulse-` 前缀按 tick 定时发送给 APP（tick 间隔通常 100ms）。

## 服务端 → APP 消息

统一为 `type:"msg"`。

### 强度操作指令

`message = strength - 通道 + 模式 + 数值`：通道 1=A/2=B；模式 0=减少/1=增加/2=设为指定值；数值 0~200。

示例：`strength-1+2+35` → A 通道强度设为 35；`strength-2+2+0` → B 通道归零

### 波形操作指令

`message = pulse - 通道:["HEX 帧",...]`：8 字节 HEX 一帧 = 100ms；数组最大 100 帧（10s）；APP 缓存上限 500 帧（50s）。波形数据参考 `socket/DG_WAVES_V2_V3_simple.js` 的 `expectedV3`。

### 清空波形队列

`message = clear - 通道`：`clear-1` 清 A、`clear-2` 清 B。建议清空后稍等片刻再发新波形以规避网络延迟丢包。

## APP → 服务端 → 前端消息

- 强度回传：`strength-A强度+B强度+A上限+B上限`（值 0~200）
- 反馈按钮：`feedback-角标`（0~4 A 通道按钮，5~9 B 通道按钮）

## 服务端 → 前端控制消息

- 绑定：`type:"bind"`（含目标为空/成功 200/失败 400 三种形态）
- 断开：`type:"break"` message 209
- 错误：`type:"error"` message 402
- 心跳：`type:"heartbeat"` message 200（默认 60s 一次）

## 二维码协议校验规则

1. 必须包含 `https://www.dungeon-lab.com/app-download.php`
2. 必须包含标签 `DGLAB-SOCKET`
3. 必须包含 SOCKET 服务地址+终端 ID，中间不得有额外路径
4. 有且仅有两个 `#` 分隔三部分
5. 本地调试用 `ws://`，正式推荐 `wss://`

## 错误码

| 码 | 含义 |
|---|---|
| 200 | 成功 |
| 209 | 对方已断开 |
| 210 | 二维码中无有效 clientID |
| 211 | 连接成功但未下发 APP ID |
| 400 | ID 已被其他客户端绑定 |
| 401 | 目标客户端不存在 |
| 402 | 收发双方非绑定关系 |
| 403 | 内容非标准 JSON |
| 404 | 收信人离线 |
| 405 | message 长度超 1950 |
| 406 | 缺少 channel 字段 |
| 500 | 服务器内部异常 |

## 与本项目（Phira）的接入映射

| 设置项（Config） | 用途 |
|---|---|
| `dglab_enabled` | 总开关：是否连接郊狼 |
| `dglab_use_ble` | 传输方式：false=WebSocket / true=BLE（默认 false） |
| `dglab_ws_url` | WebSocket 服务地址（连官方参考服务或自建服务，默认端口 9999） |
| `dglab_perfect_power` | Perfect 判定时发送的强度（0~200） |
| `dglab_good_power` | Good 判定时发送的强度（0~200） |
| `dglab_badmiss_power` | Bad/Miss 判定时发送的强度（0~200） |

判定事件经判定钩子（`prpr::scene::game::UpdateFn`）转发到 WS 任务，映射为 V2 协议的 `type:3` 强度设置指令（通道 A/B 同时设置同值；后续可扩展为波形下发）。