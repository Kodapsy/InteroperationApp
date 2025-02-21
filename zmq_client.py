import zmq
import json
context = zmq.Context()
socket = context.socket(zmq.SUB)  # 使用 SUB 套接字订阅消息
socket.connect("tcp://192.168.20.224:27170")  # 连接到服务器

socket.setsockopt_string(zmq.SUBSCRIBE, "")

while True:
    # 将消息解析为 JSON 格式
    message = socket.recv_string()
    try:
        parsed_message = json.loads(message)
        # 检查消息的 Topic 是否匹配
        if parsed_message.get("Topic") == "12345":
            print(f"Received message: {parsed_message}")
        else:
            print(f"Message topic does not match: {parsed_message.get('Topic')}")
    except json.JSONDecodeError:
        print(f"Failed to decode message: {message}")
