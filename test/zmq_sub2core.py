import argparse
import zmq
import threading
import InteroperationApp.czlconfig as czlconfig

def zmq_core_sub_server(port,topic):
    """ZMQ 服务器线程，绑定到指定 IP 和端口"""
    context = zmq.Context()
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{czlconfig.selfip}:{czlconfig.recv_sub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
    print(f"[ZMQ Server] 服务器启动，监听 {czlconfig.selfip}:{czlconfig.recv_sub_port}")
    
    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{czlconfig.selfip}:{port}")  # 连接 XPUB 代理-代理的发送端口
    
    while True:
        message = sub_socket.recv_string()
        print(f"[ZMQ Server] 收到消息了")
        #可以有中间处理~
        pub_socket.send_string(message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="示例: 通过命令行传递Port、Topic")
    parser.add_argument("--port", type=int, required=True, help="端口号")
    parser.add_argument("--topic", type=str, required=True, help="主题")
    args = parser.parse_args()

    # 启动新线程运行 ZMQ 服务器
    server_thread = threading.Thread(target=zmq_core_sub_server, args=(args.port,args.topic), daemon=True)
    server_thread.start()

    print("[Main] ZMQ 服务器已启动，线程ID:", server_thread.ident)
    
    # 让主线程保持运行，防止程序退出
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\n[Main] 程序已退出")
