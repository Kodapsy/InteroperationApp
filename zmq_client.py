import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)  # 使用 SUB 套接字订阅消息
socket.connect("tcp://10.112.62.26:27130")  # 连接到服务器

# 设置订阅所有消息
socket.setsockopt_string(zmq.SUBSCRIBE, "")

while True:
    message = socket.recv_string()  # 接收服务器广播的消息
    print(f"Received message: {message}")
