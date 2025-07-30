import socket
import json
import time
import threading
import os
data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
def query_ncs_status_and_listen(broadcast_ip="192.168.20.198", port=50500, listen_ip="192.168.20.223", listen_port=50501, output_file=os.path.join(data_dir,"output.json"),output_file2=os.path.join(data_dir,"output2.json")):
    """
    查询NCS状态并在收到2002回复后，启动监听功能。
    持续监听2101消息，将data数据写入文件，每分钟发送心跳信息(tag==2005)。
    """
    # 创建UDP套接字
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    # 构造请求消息
    request_message = json.dumps({"tag": 2001, "data": {"ip": listen_ip, "port": listen_port}})
    #request_message2 = json.dumps({"tag": 1001})
    try:
        # 发送广播消息
        sock.sendto(request_message.encode('utf-8'), (broadcast_ip, port))
        #sock.sendto(request_message2.encode('utf-8'), (broadcast_ip, port))
        # 设置超时时间
        sock.settimeout(5)

        # 接收响应消息
        response, addr = sock.recvfrom(1024)
        response_data = json.loads(response.decode('utf-8'))

        # 检查是否收到2002的回复
        if response_data.get("tag") == 2002:
            # 启动心跳线程
            #print(response_data)
           # if(response_data.get("rsp") != 0):
             #   print("error 2002")
            #    return

            unique_id = response_data.get("unique")
            heartbeat_thread = threading.Thread(target=send_heartbeat, args=(listen_ip, listen_port,unique_id))
            heartbeat_thread.daemon = True
            heartbeat_thread.start()

            # 启动监听
            listen_vehicle_info(listen_ip, listen_port, output_file,output_file2)
        else:
            print(response_data)
    except socket.timeout:
        print("请求超时，未收到响应。")
    finally:
        sock.close()

def send_heartbeat(listen_ip, listen_port,unique_id):
    """
    定时发送心跳信息 (tag == 2005)。
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        heartbeat_message = json.dumps({"tag": 2005, "unique": unique_id})
        sock.sendto(heartbeat_message.encode('utf-8'), (listen_ip, listen_port))
        sock.settimeout(3)
        print("心跳信息已发送")
        response,addr=sock.recvfrom(1024)
        response_data = json.loads(response.decode('utf-8'))
        printf(response_data)
        time.sleep(60)  # 每60秒发送一次心跳

def listen_vehicle_info(listen_ip, listen_port, output_file,output_file2):
    """
    持续监听本车节点信息上报。
    收到2101消息后将data内容写入文件。
    """
    # 创建UDP套接字
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((listen_ip, listen_port))

    try:
        while True:
            # 接收消息
            data, addr = sock.recvfrom(4096)

            # 解析JSON数据
            try:
                message = json.loads(data.decode('utf-8'))
                tag = message.get("tag",-1)
                print(tag)
                if message.get("tag") == 2101:  # 确认是本车节点信息上报
                    # 获取data并写入文件
                    self_data = message.get("data", {})
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(self_data, f, indent=4, ensure_ascii=False)
                    #print("2101消息已记录：", self_data)
                if message.get("tag") == 3001:
                    self_data = message.get("data", {})
                    with open(output_file2, "w", encoding="utf-8") as f:
                        json.dump(self_data, f, indent=4, ensure_ascii=False)
                if message.get("tag") == 2006:
                    print(message)
            except json.JSONDecodeError:
                print("JSON消息解析失败。")
    except KeyboardInterrupt:
        print("手动停止监听。")
    finally:
        sock.close()

if __name__ == "__main__":
    # 启动查询和监听功能
    query_ncs_status_and_listen()
