import threading
import zmq
import json
import os
import sys
import uuid
import glob
import time
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import module.CapabilityManager as CapabilityManager
import module.CollaborationGraphManager as CollaborationGraphManager
import config
# 创建数据目录
os.makedirs(config.data_dir, exist_ok=True)

# 最大数据包大小
MAX_SIZE = 1.4 * 1024
MAX_FILES = 12

# ZMQ 上下文
context = zmq.Context()

def clean_old_files():
    """如果 JSON 文件超过 MAX_FILES，删除最早的文件"""
    json_files = sorted(glob.glob(os.path.join(config.data_dir, "*.json")), key=os.path.getctime)  # 按创建时间排序
    while len(json_files) > MAX_FILES:
        oldest_file = json_files.pop(0)  # 取出最早的文件
        try:
            os.remove(oldest_file)
            print(f"[✘] Deleted old file: {oldest_file}")
        except Exception as e:
            print(f"[!] Error deleting file {oldest_file}: {e}")

def core():
    """ZeroMQ 代理线程 (XSUB -> XPUB)"""
    corexsub = context.socket(zmq.XSUB)
    corexsub.bind(f"tcp://*:{config.send_sub_port}") 

    relayxpub = context.socket(zmq.XPUB)
    relayxpub.bind(f"tcp://*:{config.send_pub_port}") 

    corexpub = context.socket(zmq.XPUB)
    corexpub.bind(f"tcp://*:{config.recv_pub_port}")

    relayxsub = context.socket(zmq.XSUB)
    relayxsub.bind(f"tcp://*:{config.recv_sub_port}")

    print("[Core] ZeroMQ 代理启动...")

    # 使用 zmq.proxy 进行消息转发
    zmq.proxy(corexsub, relayxpub)
    zmq.proxy(corexpub, relayxsub)

def core_pub2obu():
    """监听 `send_pub_port` 并转发到 `obu_sub_port`"""
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{config.selfip}:{config.send_pub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{config.obu_sub_port}")

    print("[core_pub2obu] 线程启动，等待消息...")

    count = 0
    last_timer_send = time.time()
    while True:
        try:
            if sub_socket.poll(100):
                message = sub_socket.recv_string()
                print(f"[Core] 收到消息 {count}")
                count += 1
                # 消息处理
                data = json.loads(message)
                if data["Send Type"] == 0:
                    if data["Message Type"] == config.pub_type:
                        content = data["Data"]
                        #print(len(content))
                        if len(content) > MAX_SIZE:
                            filename = f"{config.data_dir}/{uuid.uuid4()}.json"
                            with open(filename, "w", encoding="utf-8") as f:
                                json.dump(data, f, indent=4)
                            data["Data"] = "use itp"
                            data["File Path"] = filename
                            clean_old_files()
                        message = json.dumps(data)
                        print(message)
                        pub_socket.send_string(message)
                    elif data["Message Type"] == config.sub_type:
                        message = json.dumps(data)
                        pub_socket.send_string(message)
                    else:
                        print(f"[!] 未知消息类型: {data['Message Type']}")
                        sub_socket.send_string("[!] 未知消息类型")
                elif data["Send Type"] == 1:
                    if(data["Cap Operator"] == 0): # 添加能力
                        CapabilityManager.CapabilityManager.putCapability(data["ApplicationIdentifier"], data["Cap ID"], data["Cap Version"], data["Cap Configuration"])
                        sub_socket.send_string("[✔] 添加能力成功")
                    elif(data["Cap Operator"] == 1): # 删除能力
                        CapabilityManager.CapabilityManager.deleteCapability(data["ApplicationIdentifier"], data["Cap ID"], data["Cap Version"], data["Cap Configuration"])
                        sub_socket.send_string("[✔] 删除能力成功")
                    elif(data["Cap Operator"] == 2): # 获取协作图
                        Devices = CollaborationGraphManager.CollaborationGraphManager.getDevices(data["Cap ID"], data["Cap Version"], data["Cap Configuration"])
                        sub_socket.send_string(json.dumps(Devices))
                    else:
                        print(f"[!] 未知操作类型: {data['Cap Operator']}")
                        sub_socket.send_string("[!] 未知操作类型")
                else:
                    print(f"[!] 未知发送类型: {data['Send Type']}")
                    sub_socket.send_string("[!] 未知发送类型")
            if time.time() - last_timer_send >= config.echo_time:
                capsList = CapabilityManager.CapabilityManager.getCapability()
                caps = len(capsList)
                echo_message = {
                    "Message Type": config.echo_type,
                    "CapsList": capsList,
                    "Caps": caps,
                    "Source Vehicle ID": config.source_id,
                    "Peer Vehicle ID": config.board_id
                }
                pub_socket.send_string(json.dumps(echo_message))
                print(f"[Core] 发送 echo 消息: {echo_message}")
                last_timer_send = time.time()
        except Exception as e:
            print(f"[!] core_pub2obu 发生错误: {e}")

def core_sub2obu():
    """监听 `obu_pub_port` 并转发到 `recv_sub_port`"""
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{config.selfip}:{config.obu_pub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    pub_socket = context.socket(zmq.PUB)
    pub_socket.connect(f"tcp://{config.selfip}:{config.recv_sub_port}")

    print("[core_sub2obu] 线程启动，等待消息...")

    count = 0
    while True:
        try:
            message = sub_socket.recv_string()
            print(f"[Core] 收到消息 {count}: {message}")
            count += 1
            data = json.loads(message)
            # 消息处理
            if data["Message Type"] == config.echo_type:
                CollaborationGraphManager.CollaborationGraphManager.getInstance().updateMapping(data["Source Vehicle ID"], data["CapsList"])
            elif data["Message Type"] == config.sub_type or data["Message Type"] == config.pub_type:
                topic = data["Topic"]
                topic_prefixed_message = f"{topic} {json.dumps(data, ensure_ascii=False)}"
                pub_socket.send_string(topic_prefixed_message)
            else:
                print(f"[!] 未知消息类型: {data['Message Type']}")
        except Exception as e:
            print(f"[!] core_sub2obu 发生错误: {e}")

def core_echoPub():
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{config.obu_sub_port}")
    

def main():
    """启动所有线程"""
    threads = []

    # 启动 ZeroMQ 代理
    t_core = threading.Thread(target=core, daemon=True)
    threads.append(t_core)

    # 启动 PUB-SUB 处理线程
    t_pub2obu = threading.Thread(target=core_pub2obu, daemon=True)
    threads.append(t_pub2obu)

    t_sub2obu = threading.Thread(target=core_sub2obu, daemon=True)
    threads.append(t_sub2obu)

    # 启动所有线程
    for t in threads:
        t.start()

    print("[Main] 所有线程已启动，等待消息...")

    # 监听 Ctrl + C 退出
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[✔] 程序已退出")
        exit(0)

if __name__ == "__main__":
    main()
