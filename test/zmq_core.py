import threading
import zmq
import json
import os
import sys
import uuid
import glob
import time
sys.path.append("/home/nvidia/mydisk/czl/InteroperationApp")
#sys.path.append("/home/czl/InteroperationApp")
import module.CapabilityManager as CapabilityManager
import module.CollaborationGraphManager as CollaborationGraphManager
import config
import module.TLV
from module.sessionManager import SessionManager
import queue
# 创建数据目录
os.makedirs(config.data_dir, exist_ok=True)

# 最大数据包大小
MAX_SIZE = 1.4 * 1024
MAX_FILES = 12
caps_instance = CapabilityManager.CapabilityManager()
maps_instance = CollaborationGraphManager.CollaborationGraphManager()
SM_instance = SessionManager.getInstance()
# ZMQ 上下文
context = zmq.Context()
pub2obu_queue = queue.Queue()
def pub2obu_loop():
    """唯一绑定 obu_sub_port 的线程，负责转发消息到 OBU"""
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{config.obu_sub_port}")
    print("[pub2obu_loop] 启动，等待消息发送到 OBU...")

    while True:
        try:
            msg = pub2obu_queue.get()  # 从队列中取出消息（阻塞）
            print(f"[pub2obu_loop] 发送消息: {msg}")
            pub_socket.send_json(msg)
        except Exception as e:
            print(f"[!] pub2obu_loop 发送失败: {e}")
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

def proxy_send():
    corexsub = context.socket(zmq.XSUB)
    corexsub.bind(f"tcp://*:{config.send_sub_port}") 
    relayxpub = context.socket(zmq.XPUB)
    relayxpub.bind(f"tcp://*:{config.send_pub_port}") 
    zmq.proxy(corexsub, relayxpub)

def proxy_recv():
    corexpub = context.socket(zmq.XPUB)
    corexpub.bind(f"tcp://*:{config.recv_pub_port}")
    relayxsub = context.socket(zmq.XSUB)
    relayxsub.bind(f"tcp://*:{config.recv_sub_port}")
    zmq.proxy(corexpub, relayxsub)


def core_sub2app():
    """监听 `send_pub_port` """
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{config.selfip}:{config.send_pub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    pub2app_socket = context.socket(zmq.PUB)
    pub2app_socket.connect(f"tcp://{config.selfip}:{config.recv_sub_port}")

    print("[core_sub2app] 线程启动，等待消息...")

    count = 0
    last_timer_send = time.time()
    while True:
        try:
            if sub_socket.poll(100):
                message = sub_socket.recv_string()
                print(f"[DEBUG] 收到原始消息: {message}")
                try:
                    message = json.loads(message)
                except json.JSONDecodeError:
                    print(f"[!] JSON 解码失败，跳过：{message}")
                    continue

                if not isinstance(message, dict):
                    print(f"[!] 消息不是字典类型，跳过：{message}")
                    continue

                if "mid" not in message:
                    print(f"[!] 消息缺少 'mid' 字段，跳过：{message}")
                    continue
                print(f"[Core] 收到消息 {count}")
                count += 1
                # 消息处理
                mid = message["mid"]
                appid = message["app_id"]
                match mid:
                    case config.appReg:
                        tid = message["tid"]
                        data = message["msg"]
                        msg = { "tid" : tid }
                        if data["act"] == config.appActLogin:
                            flag = caps_instance.putCapability(appid, data["capId"], data["capVersion"], data["capConfig"])
                            msg["result"] = config.regack if flag else config.regnack
                        if data["act"] == config.appActLogout:
                            flag = caps_instance.deleteCapability(appid, data["capId"], data["capVersion"], data["capConfig"])
                            msg["result"] = config.delack if flag else config.delnack
                        if data["act"] == config.appActopen:
                            flag = caps_instance.updateBroadcast(appid, data["capId"], data["capVersion"], data["capConfig"], True)
                            msg["result"] = config.openack if flag else config.opennack
                        if data["act"] == config.appActclose:
                            flag = caps_instance.updateBroadcast(appid, data["capId"], data["capVersion"], data["capConfig"], False)
                            msg["result"] = config.closeack if flag else config.closenack
                        pub2app_socket.send_string(json.dumps(msg, ensure_ascii=False)) 
                    case config.boardCastPub:
                        data = message["msg"]
                        sendMsg = config.pubMsg.copy()
                        sendMsg["RT"] = 0
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = ""
                        sendMsg["OP"] = 0
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = config.type_common
                        sendMsg["EncodeMode"] = config.encodeASN
                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "Mid": config.boardCastPub
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        pub2obu_queue.put(sendMsg)
                        
                    case config.boardCastSub:
                        data = message["msg"]
                        #会话管理。。。
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            print(f"当前会话状态: {SM_instance.sessions}")
                        else:
                            print(f"会话状态更新失败")
                        sendMsg = config.subMsg.copy()
                        sendMsg["RT"] = 0
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = ""
                        sendMsg["OP"] = 0
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = config.type_common
                        sendMsg["EncodeMode"] = config.encodeASN
                        context_id = data["context"]
                        # 清理空白字符、只保留0和1
                        # 强制修剪为64位
                        print(len(context_id))
                        if len(context_id) > 64:
                            print(f"[!] ContextId 超长，已截断: 原始 {len(context_id)} → 64")
                            context_id = context_id[:64]
                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":1 if data["bearCap"] == 1 else 0,
                            "ContextId":context_id,
                            "Mid": config.boardCastSub
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        pub2obu_queue.put(sendMsg)
                    
                    case config.boardCastSubNotify:
                        data = message["msg"]
                        #会话管理。。。
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            print(f"当前会话状态: {SM_instance.sessions}")
                        else:
                            print(f"会话状态更新失败")
                        sendMsg = config.pubMsg.copy()
                        sendMsg["RT"] = 1
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["OP"] = 0
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = config.type_common
                        sendMsg["EncodeMode"] = config.encodeASN
                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":1 if data["bearCap"] == 1 else 0,
                            "ContextId":data["context"],
                            "Mid":config.boardCastSub
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        pub2obu_queue.put(sendMsg)
                    
                    case config.subScribe:
                        data = message["msg"]
                        #会话管理
                        smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                        if smFlag:
                            print(f"当前会话状态: {SM_instance.sessions}")
                        else:
                            print(f"会话状态更新失败")
                        sendMsg = config.subMsg.copy()
                        sendMsg["RT"] = 1
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["OP"] = data["act"]
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = config.type_common
                        sendMsg["EncodeMode"] = config.encodeASN
                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":2 if data["bearinfo"] == 1 else 0,
                            "ContextId":data["context"],
                            "Mid":config.subScribe
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        pub2obu_queue.put(sendMsg)
                        
                    case config.notify:
                        data = message["msg"]
                        #会话管理
                        smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                        if smFlag:
                            print(f"当前会话状态: {SM_instance.sessions}")
                        else:
                            print(f"会话状态更新失败")
                        sendMsg = config.pubMsg.copy()
                        sendMsg["RT"] = 1
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["OP"] = data["act"]
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = config.type_common
                        sendMsg["EncodeMode"] = config.encodeASN
                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":1 if data["bearCap"] == 1 else 0,
                            "ContextId":data["context"],
                            "Mid":config.notify
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        pub2obu_queue.put(sendMsg)
                        
                    #todo：流数据处理 101-107
                    case config.streamSendreq:
                        data = message["msg"]
                        #会话管理
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            print(f"当前会话状态: {SM_instance.sessions}")
                        else:
                            print(f"会话状态更新失败")
                        sendMsg = {}
                        sendMsg["RL"] = data["rl"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["PT"] = data["pt"]
                        sendMsg["context"] = data["context"]
                        sendMsg["mid"] = message["mid"]
                        pub2obu_queue.put(sendMsg)
                    case config.streamSend:
                        data = message["msg"]
                        sendMsg = {}
                        sendMsg["sid"] = data["sid"]
                        sendMsg["data"] = data["data"]
                        sendMsg["mid"] = message["mid"]
                        pub2obu_queue.put(sendMsg)
                    case config.streamSendend:
                        data = message["msg"]
                        #会话管理
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            print(f"当前会话状态: {SM_instance.sessions}")
                        else:
                            print(f"会话状态更新失败")
                        sendMsg = {}
                        sendMsg["sid"] = data["sid"]
                        sendMsg["did"] = data["did"]
                        sendMsg["context"] = data["context"]
                        sendMsg["mid"] = message["mid"]
                        pub2obu_queue.put(sendMsg)
                    #todo：文件处理 111-113
                        
                        
            """if time.time() - last_timer_send >= config.echo_time:
                capsList = caps_instance.getCapability()
                caps = len(capsList)
                echo_message = {
                    "Message Type": config.echo_type,
                    "CapsList": capsList,
                    "Caps": caps,
                    "Source Vehicle ID": config.source_id,
                    "Peer Vehicle ID": config.board_id
                }
                pub2obu_queue.put(sendMsg)
                print(f"[Core] 发送 echo 消息: {echo_message}")
                last_timer_send = time.time()"""
        except Exception as e:
            print(f"[!] core_sub2app 发生错误: {e}")

def core_sub2obu():
    """监听 `obu_pub_port` 并转发到 `recv_sub_port`"""
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{config.selfip}:{config.obu_pub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    pub2app_socket = context.socket(zmq.PUB)
    pub2app_socket.connect(f"tcp://{config.selfip}:{config.recv_sub_port}")

    print("[core_sub2obu] 线程启动，等待消息...")

    count = 0
    while True:
        try:
            message = sub_socket.recv_string()
            print(f"[Core] 收到消息 {count}: {message}")
            count += 1
            data = json.loads(message)
            message_type = data.get("Message_type")
            # 消息处理
            if message_type == config.echo_type:
                CollaborationGraphManager.CollaborationGraphManager.getInstance().updateMapping(data["Source Vehicle ID"], data["CapsList"])
            elif message_type == config.sub_type or message_type == config.pub_type:
                TLVm = data["Payload"]
                TLVmsg = module.TLV.TLVEncoderDecoder.decode(TLVm)
                if TLVmsg.get("ContextId") is None :
                    pass
                elif TLVmsg["Mid"] != config.subScribe or TLVmsg["Mid"] != config.notify:
                    SM_instance.update_state(TLVmsg["Mid"], TLVmsg["ContextId"])
                else:
                    SM_instance.update_state(TLVmsg["Mid"], TLVmsg["ContextId"], data["OP"])
                topic = data["Topic"]
                topic_prefixed_message = f"{topic} {json.dumps(TLVmsg, ensure_ascii=False)}"
                pub2app_socket.send_string(topic_prefixed_message)
            elif message_type == config.sendreq_type:
                sendMsg = {}
                sendMsg["DestId"] = data["did"]
                sendMsg["context"] = data["context"]
                sendMsg["sid"] = data["sid"]
                sendMsg["mid"] = data["mid"]
                pub2app_socket.send_string(json.dumps(sendMsg, ensure_ascii=False))
                pubMsg = config.pubMsg.copy()
                pubMsg["RT"] = 0
                pubMsg["SourceId"] = config.source_id
                pubMsg["DestId"] = data["did"]
                pubMsg["OP"] = 0
                pubMsg["PayloadType"] = config.type_common
                pubMsg["EncodeMode"] = config.encodeASN
                TLVmsg = {
                    "StreamId":data["sid"],
                    "ContextId":data["context"],
                    "Mid": config.streamSendrdy
                }
                TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                pubMsg["Payload"] = TLVm
                byte_TLV = TLVm.encode("utf-8")
                pubMsg["PayloadLength"] = len(byte_TLV)
                pub2obu_queue.put(pubMsg)
            elif message_type == config.streamRecv:
                sendMsg = {}
                sendMsg["sid"] = data["sid"]
                sendMsg["data"] = data["data"]
                sendMsg["mid"] = data["mid"]
                topic = ""
                topic_prefixed_message = f"{topic} {json.dumps(sendMsg, ensure_ascii=False)}"
                pub2app_socket.send_string(topic_prefixed_message)
            else:
                print(f"[!] 未知消息类型: {data['Message_type']}")
        except Exception as e:
            print(f"[!] core_sub2obu 发生错误: {e}")

def core_echoPub():
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{config.obu_sub_port}")
    

def main():
    """启动所有线程"""
    threads = []

    # 启动 ZeroMQ 代理
    t_proxy_send = threading.Thread(target=proxy_send, daemon=True)
    threads.append(t_proxy_send)
    
    t_proxy_recv = threading.Thread(target=proxy_recv, daemon=True)
    threads.append(t_proxy_recv)
    
    # 启动 pub2obu_router
    t_pub2obu = threading.Thread(target=pub2obu_loop, daemon=True)
    threads.append(t_pub2obu)
    
    # 启动 PUB-SUB 处理线程
    t_sub2app = threading.Thread(target=core_sub2app, daemon=True)
    threads.append(t_sub2app)

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
