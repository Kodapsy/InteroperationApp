# Python ZMQ 核心程序与 UniFrame 集成指南

本文档旨在指导用户如何配置和运行一个基于 Python 的 ZMQ 消息处理核心程序 (`zmq_core.py`)，并将其与 UniFrame 系统进行集成。

---

## 1. 环境准备

在开始之前，请确保开发环境满足以下要求：

*   **Python 版本**: `3.12.4` 或兼容版本。
*   **UniFrame 版本**: `UniFrame_11761` 或正在使用的特定版本。
    *   *注意：本文档假设 UniFrame 已经成功安装，安装过程在此略过。*
*   **依赖库**: 确保 `zmq_core.py` 及其依赖的 Python 库（如 `pyzmq`）已正确安装。

---

## 2. 消息处理核心程序 (`zmq_core.py`) 配置

核心程序 `~/test/zmq_core.py` 负责处理基于 ZMQ 的消息收发。

### 2.1 启动核心程序

首先，需要能够运行此 Python 脚本。通常，在终端中执行：

```bash
python ~/test/zmq_core.py
```
### 2.2 集成 ZMQ 服务模块
在代码的适当位置（通常是脚本的初始化部分或主逻辑之前）加入以下代码片段来实例化 ZMQ 服务端和/或客户端。
引入模块:
```bash
from module.zmq_server import ICPServer, ICPClient
```
a. 初始化 ICPServer (用于发送消息):
如果需要程序主动向 UniFrame 或其他应用发送消息，请实例化 ICPServer：
```bash
# ... 其他代码 ...

# 初始化 ICPServer 实例
# 请将 'your_app_id' 替换为应用程序实际的 AppID
# AppID 用于标识消息的来源或特定应用
server = ICPServer(app_id='your_app_id')

# ... 其他代码 ...
```
b. 初始化 ICPClient (用于接收消息):
如果需要程序监听并接收来自 UniFrame 或其他应用的消息，请实例化 ICPClient：
```bash
# ... 其他代码 ...

# 初始化 ICPClient 实例
# 请将 "your_target_topic" 替换为需要监听的实际 Topic 名称
# Topic 用于消息的分类和路由
client = ICPClient(topic="your_target_topic")

# ... 其他代码 ...
```
## 3. 使用示例
### 3.1 服务端 (ICPServer) - 发送应用消息
通过已初始化的 server 对象，可以调用 AppMessage 方法来发送消息。
```bash
# 假设 server 已经按照 2.2.a 中的方式初始化

# 定义消息参数 (请根据实际应用替换这些值)
CapID = "unique_capability_id"      # 能力ID
CapVersion = "1.0"                  # 能力版本
CapConfig = {"param1": "value1"}    # 能力配置 (通常是字典或JSON字符串)
act = "request"                     # 动作类型 (如 "request", "response", "notify")
tid = "transaction_id_123"          # 事务ID，用于跟踪请求和响应

# 发送消息
server.AppMessage(CapID, CapVersion, CapConfig, act, tid)
print(f"消息已通过 ICPServer 发送: CapID={CapID}, TID={tid}")
Use code with caution.
Python
CapID, CapVersion, CapConfig, act, tid 的具体含义和格式取决于应用协议和 UniFrame 的配置。
```
3.2 客户端 (ICPClient) - 接收消息
通过已初始化的 client 对象，可以调用 recv_message 方法来监听并接收消息。此方法通常是阻塞的，直到收到消息为止。
```bash
# 假设 client 已经按照 2.2.b 中的方式初始化

print(f"ICPClient 开始监听 Topic: '{client.topic}'...")
try:
    while True: # 可以根据需要调整循环条件
        # 接收消息 (阻塞操作)
        received_message = client.recv_message()

        if received_message:
            print(f"从 Topic '{client.topic}' 收到消息: {received_message}")
            # 在这里处理 received_message
            # 例如: 解析消息内容，根据消息类型执行不同操作等
        else:
            # recv_message 可能在某些情况下返回 None (例如超时或关闭时)
            print("未收到消息或连接已关闭。")
            break
except KeyboardInterrupt:
    print("用户中断了消息接收。")
finally:
    # 可以在这里添加清理代码，例如关闭客户端连接
    # client.close() # (如果 ICPClient 有 close 方法)
    print("ICPClient 停止监听。")
```
## 4. 启动 UniFrame 并与之交互
在 Python 核心程序 (zmq_core.py) 配置完成并运行后，可以启动 UniFrame 并与之交互。
打开一个新的终端窗口。
启动 UniFrame 服务控制器:
```bash
sc0
```
这通常会启动 UniFrame 的主控制进程。
进入应用管理实体 (AME) 或其他相关界面:
```bash
ame 1
```
此命令的具体作用取决于 UniFrame 的实现