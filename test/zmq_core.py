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
from module.CapabilityManager import CapabilityManager
from module.CollaborationGraphManager import CollaborationGraphManager
import config
import module.TLV
from module.sessionManager import SessionManager
from module.logger import global_logger # Assuming global_logger is defined in module/logger.py
import queue

# 创建数据目录
os.makedirs(config.data_dir, exist_ok=True)

# --- 为每个主要功能模块获取独立的 logger 实例 ---
logger_main = global_logger.get_logger("main") 
logger_pub2obu = global_logger.get_logger("pub2obu")
logger_cleaner = global_logger.get_logger("cleaner")
logger_proxy_send = global_logger.get_logger("proxy_send")
logger_proxy_recv = global_logger.get_logger("proxy_recv")
logger_core_app = global_logger.get_logger("core_app")
logger_core_obu = global_logger.get_logger("core_obu")

# --- 启用这些模块 (使用你 Logger 版本中的 enable_module) ---
# If your get_logger auto-enables, this might be redundant but clear.
global_logger.enable_module("main")
global_logger.enable_module("pub2obu")
global_logger.enable_module("cleaner")
global_logger.enable_module("proxy_send")
global_logger.enable_module("proxy_recv")
global_logger.enable_module("core_app")
global_logger.enable_module("core_obu")


# 最大数据包大小 (This seems unused directly in this file, but kept from original)
MAX_SIZE = 1.4 * 1024 
MAX_FILES = 12 # Used in clean_old_files
caps_instance = CapabilityManager.getInstance()
maps_instance = CollaborationGraphManager.getInstance() # Used in core_sub2obu
SM_instance = SessionManager.getInstance()
# ZMQ 上下文
context = zmq.Context()
pub2obu_queue = queue.Queue()

def pub2obu_loop():
    """唯一绑定 obu_sub_port 的线程，负责转发消息到 OBU"""
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{config.obu_sub_port}")
    logger_pub2obu.info("[pub2obu_loop] 启动，等待消息发送到 OBU...")

    while True:
        try:
            msg = pub2obu_queue.get()  # 从队列中取出消息（阻塞）
            logger_pub2obu.info(f"[pub2obu_loop] 发送消息: {msg}")
            pub_socket.send_json(msg)
        except Exception as e:
            logger_pub2obu.error(f"[!] pub2obu_loop 发送失败: {e}")

def clean_old_files():
    """如果 JSON 文件超过 MAX_FILES，删除最早的文件"""
    logger_cleaner.debug(f"检查旧文件，目录: {config.data_dir}, MAX_FILES: {MAX_FILES}")
    json_files = sorted(glob.glob(os.path.join(config.data_dir, "*.json")), key=os.path.getctime)  # 按创建时间排序
    logger_cleaner.debug(f"找到 {len(json_files)} 个 JSON 文件。")
    while len(json_files) > MAX_FILES:
        oldest_file = json_files.pop(0)  # 取出最早的文件
        logger_cleaner.info(f"文件数量超限，准备删除: {oldest_file}")
        try:
            os.remove(oldest_file)
            logger_cleaner.info(f"[✘] Deleted old file: {oldest_file}")
        except Exception as e:
            logger_cleaner.error(f"[!] Error deleting file {oldest_file}: {e}")

def proxy_send():
    corexsub = context.socket(zmq.XSUB)
    corexsub.bind(f"tcp://*:{config.send_sub_port}") 
    relayxpub = context.socket(zmq.XPUB)
    relayxpub.bind(f"tcp://*:{config.send_pub_port}") 
    logger_proxy_send.info("[proxy_send] 代理启动，等待消息...")
    try:
        zmq.proxy(corexsub, relayxpub)
    except zmq.ContextTerminated:
        logger_proxy_send.info("[proxy_send] ZMQ 上下文已终止，代理关闭。")
    except Exception as e:
        logger_proxy_send.error(f"[proxy_send] 代理发生错误并退出: {e}", exc_info=True)


def proxy_recv():
    corexpub = context.socket(zmq.XPUB)
    corexpub.bind(f"tcp://*:{config.recv_pub_port}")
    relayxsub = context.socket(zmq.XSUB)
    relayxsub.bind(f"tcp://*:{config.recv_sub_port}")
    logger_proxy_recv.info("[proxy_recv] 代理启动，等待消息...")
    try:
        zmq.proxy(corexpub, relayxsub)
    except zmq.ContextTerminated:
        logger_proxy_recv.info("[proxy_recv] ZMQ 上下文已终止，代理关闭。")
    except Exception as e:
        logger_proxy_recv.error(f"[proxy_recv] 代理发生错误并退出: {e}", exc_info=True)


def core_sub2app():
    """监听 `send_pub_port` """
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{config.selfip}:{config.send_pub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    pub2app_socket = context.socket(zmq.PUB)
    pub2app_socket.connect(f"tcp://{config.selfip}:{config.recv_sub_port}")

    logger_core_app.info("[core_sub2app] 线程启动，等待消息...")

    count = 0
    # last_timer_send = time.time() # Uncomment if echo logic is restored
    while True:
        try:
            if sub_socket.poll(100): # Timeout of 100ms
                message_str = sub_socket.recv_string()
                logger_core_app.debug(f"[core_sub2app] 收到原始消息: {message_str[:200]}...") # Log snippet
                try:
                    message = json.loads(message_str)
                except json.JSONDecodeError:
                    logger_core_app.error(f"[core_sub2app] JSON 解码失败，跳过：{message_str}")
                    continue

                if not isinstance(message, dict):
                    logger_core_app.warning(f"[core_sub2app] 消息不是字典类型，跳过：{message}")
                    continue

                if "mid" not in message:
                    logger_core_app.warning(f"[core_sub2app] 消息缺少 'mid' 字段，跳过：{message}")
                    continue
                
                logger_core_app.info(f"[core_sub2app] 收到已解析消息 (count {count}), mid: {message.get('mid')}")
                count += 1
                
                mid = message["mid"]
                appid = message.get("app_id", "AppIdNotProvided") # Use .get for safety

                match mid:
                    case config.appReg:
                        logger_core_app.info(f"core_sub2app: 处理 appReg from appid: {appid}")
                        tid = message["tid"]
                        data = message["msg"]
                        msg_response = { "tid" : tid } # Renamed to avoid conflict
                        act = data["act"]
                        logger_core_app.debug(f"core_sub2app: appReg action: {act}")
                        if act == config.appActLogin:
                            flag = caps_instance.putCapability(appid, data["capId"], data["capVersion"], data["capConfig"])
                            msg_response["result"] = config.regack if flag else config.regnack
                        elif act == config.appActLogout: # Use elif for mutually exclusive conditions
                            flag = caps_instance.deleteCapability(appid, data["capId"], data["capVersion"], data["capConfig"])
                            msg_response["result"] = config.delack if flag else config.delnack
                        elif act == config.appActopen:
                            flag = caps_instance.updateBroadcast(appid, data["capId"], data["capVersion"], data["capConfig"], True)
                            msg_response["result"] = config.openack if flag else config.opennack
                        elif act == config.appActclose:
                            flag = caps_instance.updateBroadcast(appid, data["capId"], data["capVersion"], data["capConfig"], False)
                            msg_response["result"] = config.closeack if flag else config.closenack
                        else:
                            logger_core_app.warning(f"core_sub2app: appReg 未知 action: {act}")
                            msg_response["result"] = "NACK_UNKNOWN_ACTION"
                        logger_core_app.info(f"core_sub2app: appReg 发送响应: {msg_response}")
                        pub2app_socket.send_string(json.dumps(msg_response, ensure_ascii=False)) 
                    
                    case config.boardCastPub:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastPub")
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
                            "Mid": config.boardCastPub # Mid for TLV content
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8") # Should be TLVm.encode if TLVm is string, or direct bytes
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        logger_core_app.info(f"core_sub2app: boardCastPub, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)
                        
                    case config.boardCastSub:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastSub")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: boardCastSub 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.warning(f"core_sub2app: boardCastSub 会话状态更新失败")
                        sendMsg = config.subMsg.copy()
                        sendMsg["RT"] = 0
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = ""
                        sendMsg["OP"] = 0
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = config.type_common
                        sendMsg["EncodeMode"] = config.encodeASN
                        context_id = data["context"]
                        logger_core_app.debug(f"core_sub2app: boardCastSub 原始 context_id 长度: {len(context_id)}")
                        if len(context_id) > 64:
                            logger_core_app.warning(f"core_sub2app: ContextId 超长，已截断: 原始 {len(context_id)} → 64")
                            context_id = context_id[:64]
                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":1 if data["bearCap"] == 1 else 0,
                            "ContextId":context_id,
                            "Mid": config.boardCastSub # Mid for TLV content
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        logger_core_app.info(f"core_sub2app: boardCastSub, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)
                    
                    case config.boardCastSubNotify:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastSubNotify")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: boardCastSubNotify 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.warning(f"core_sub2app: boardCastSubNotify 会话状态更新失败")
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
                            "Mid":config.boardCastSub # Original code has boardCastSub, assuming intentional for TLV
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        logger_core_app.info(f"core_sub2app: boardCastSubNotify, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)
                    
                    case config.subScribe:
                        logger_core_app.info(f"core_sub2app: 处理 subScribe")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: subScribe 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.warning(f"core_sub2app: subScribe 会话状态更新失败")
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
                            "Mid":config.subScribe # Mid for TLV content
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        logger_core_app.info(f"core_sub2app: subScribe, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)
                        
                    case config.notify:
                        logger_core_app.info(f"core_sub2app: 处理 notify")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: notify 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.warning(f"core_sub2app: notify 会话状态更新失败")
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
                            "Mid":config.notify # Mid for TLV content
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        logger_core_app.info(f"core_sub2app: notify, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)
                        
                    case config.streamSendreq:
                        logger_core_app.info(f"core_sub2app: 处理 streamSendreq")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: streamSendreq 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.warning(f"core_sub2app: streamSendreq 会话状态更新失败")
                        sendMsg = {} # This is not the standard OBU message format from config
                        sendMsg["RL"] = data["rl"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["PT"] = data["pt"]
                        sendMsg["context"] = data["context"]
                        sendMsg["mid"] = message["mid"] # Pass original mid
                        logger_core_app.info(f"core_sub2app: streamSendreq (mid {message['mid']}), context '{data['context']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    case config.streamSend:
                        logger_core_app.info(f"core_sub2app: 处理 streamSend")
                        data = message["msg"]
                        sendMsg = {} # Not standard OBU message format
                        sendMsg["sid"] = data["sid"]
                        sendMsg["data"] = data["data"] # Potentially large
                        sendMsg["mid"] = message["mid"] # Pass original mid
                        logger_core_app.info(f"core_sub2app: streamSend (mid {message['mid']}), sid '{data['sid']}', data len {len(data.get('data',''))}, 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    case config.streamSendend:
                        logger_core_app.info(f"core_sub2app: 处理 streamSendend")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: streamSendend 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.warning(f"core_sub2app: streamSendend 会话状态更新失败")
                        sendMsg = {} # Not standard OBU message format
                        sendMsg["sid"] = data["sid"]
                        sendMsg["did"] = data["did"]
                        sendMsg["context"] = data["context"]
                        sendMsg["mid"] = message["mid"] # Pass original mid
                        logger_core_app.info(f"core_sub2app: streamSendend (mid {message['mid']}), sid '{data['sid']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)
                    
                    # You might have cases for file handling (111-113) here
                    # case config.fileTransferRequest: ...
                    # case config.fileTransferData: ...
                    # case config.fileTransferEnd: ...

                    case _: # Default case for unhandled mid
                        logger_core_app.warning(f"core_sub2app: 未处理的 mid: {mid}, 消息: {message}")
                        
            # Echo logic from original code (commented out)
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
                # sendMsg was not defined here in original if block
                # Assuming echo_message should be sent or a specific sendMsg for echo
                # pub2obu_queue.put(echo_message) # Or format as OBU message
                logger_core_app.info(f"[Core_App] 发送 echo 消息: {echo_message}")
                last_timer_send = time.time()"""
        except zmq.Again: # Poll timed out
            pass
        except zmq.ContextTerminated:
            logger_core_app.info("[core_sub2app] ZMQ 上下文终止，线程退出。")
            break
        except Exception as e:
            logger_core_app.error(f"[core_sub2app] 发生错误: {e}", exc_info=True)

def core_sub2obu():
    """监听 `obu_pub_port` 并转发到 `recv_sub_port`"""
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{config.selfip}:{config.obu_pub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    pub2app_socket = context.socket(zmq.PUB)
    pub2app_socket.connect(f"tcp://{config.selfip}:{config.recv_sub_port}")

    logger_core_obu.info("[core_sub2obu] 线程启动，等待来自 OBU 的消息...")

    count = 0
    while True:
        try:
            message_str = sub_socket.recv_string()
            logger_core_obu.info(f"[core_sub2obu] 从 OBU 收到消息 (count {count}): {message_str[:200]}...")
            count += 1
            data = json.loads(message_str)
            message_type = data.get("Message_type") # Note: original has "Message_type"
            
            if message_type == config.echo_type:
                logger_core_obu.info(f"core_sub2obu: 处理 echo_type from {data.get('Source Vehicle ID')}")
                maps_instance.updateMapping(data["Source Vehicle ID"], data["CapsList"]) # maps_instance was defined
                logger_core_obu.debug(f"core_sub2obu: echo_type, 更新了来自 {data.get('Source Vehicle ID')} 的映射")
            
            elif message_type == config.sub_type or message_type == config.pub_type:
                logger_core_obu.info(f"core_sub2obu: 处理 sub_type/pub_type, topic: {data.get('Topic')}")
                TLVm = data["Payload"]
                TLVmsg = module.TLV.TLVEncoderDecoder.decode(TLVm)
                logger_core_obu.debug(f"core_sub2obu: 解码后的 TLV: {TLVmsg}")

                context_id_tlv = TLVmsg.get("ContextId")
                mid_tlv = TLVmsg.get("Mid")

                if context_id_tlv is None :
                    logger_core_obu.warning(f"core_sub2obu: sub_type/pub_type TLV 中 ContextId 为空。 TLV: {TLVmsg}")
                # Original logic: elif TLVmsg["Mid"] != config.subScribe or TLVmsg["Mid"] != config.notify:
                # This condition is always true. Corrected to 'and' or check specific cases.
                elif mid_tlv not in (config.subScribe, config.notify): # Check if it's NOT one of these
                    SM_instance.update_state(mid_tlv, context_id_tlv)
                    logger_core_obu.debug(f"core_sub2obu: 会话状态更新 (mid {mid_tlv}, context {context_id_tlv})")
                else: # It IS subScribe or notify
                    SM_instance.update_state(mid_tlv, context_id_tlv, data.get("OP")) # OP from original ZMQ message
                    logger_core_obu.debug(f"core_sub2obu: 会话状态更新 (mid {mid_tlv}, context {context_id_tlv}, OP {data.get('OP')})")
                
                topic = data["Topic"]
                topic_prefixed_message = f"{topic} {json.dumps(TLVmsg, ensure_ascii=False)}"
                logger_core_obu.info(f"core_sub2obu: sub_type/pub_type, 转发到 APP socket, topic: {topic}")
                pub2app_socket.send_string(topic_prefixed_message)
            
            elif message_type == config.sendreq_type: # Assuming this is for Stream Send Ready from OBU
                logger_core_obu.info(f"core_sub2obu: 处理 sendreq_type (stream send ready from OBU)")
                # This part seems to be constructing a message for the app AND one for OBU (streamSendrdy)
                # Message for App:
                sendMsg_app = {}
                sendMsg_app["DestId"] = data.get("did") # Original: data["did"], assuming 'did' is source of this msg
                sendMsg_app["context"] = data.get("context")
                sendMsg_app["sid"] = data.get("sid")
                sendMsg_app["mid"] = data.get("mid") # This 'mid' is likely the OBU's mid (e.g. streamSendrdy)
                logger_core_obu.info(f"core_sub2obu: sendreq_type, 转发到 APP socket: {sendMsg_app}")
                pub2app_socket.send_string(json.dumps(sendMsg_app, ensure_ascii=False))
                
                # Message for OBU (streamSendrdy) - as per original logic
                # This implies the OBU sent a request, and we are now sending 'rdy'
                # However, the message_type is already sendreq_type (which I interpret as streamSendrdy). This might be a loop or misunderstanding.
                # For now, replicating original logic.
                logger_core_obu.info(f"core_sub2obu: sendreq_type, 准备发送 streamSendrdy (mid {config.streamSendrdy}) 到 OBU")
                pubMsg_obu = config.pubMsg.copy()
                pubMsg_obu["RT"] = 0 # Assuming request, not response
                pubMsg_obu["SourceId"] = config.source_id # Our ID
                pubMsg_obu["DestId"] = data.get("did")    # Destination is who sent the sendreq_type
                pubMsg_obu["OP"] = 0
                pubMsg_obu["PayloadType"] = config.type_common
                pubMsg_obu["EncodeMode"] = config.encodeASN
                TLVmsg_obu = {
                    "StreamId": data.get("sid"),
                    "ContextId": data.get("context"),
                    "Mid": config.streamSendrdy # Explicitly setting Mid for TLV
                }
                TLVm_obu = module.TLV.TLVEncoderDecoder.encode(TLVmsg_obu)
                pubMsg_obu["Payload"] = TLVm_obu
                byte_TLV_obu = TLVm_obu.encode("utf-8")
                pubMsg_obu["PayloadLength"] = len(byte_TLV_obu)
                logger_core_obu.info(f"core_sub2obu: sendreq_type, streamSendrdy (mid {config.streamSendrdy}) 放入 pub2obu_queue")
                pub2obu_queue.put(pubMsg_obu)

            elif message_type == config.streamRecv:
                logger_core_obu.info(f"core_sub2obu: 处理 streamRecv")
                sendMsg_app = {}
                sendMsg_app["sid"] = data.get("sid")
                sendMsg_app["data"] = data.get("data") # Potentially large
                sendMsg_app["mid"] = data.get("mid") # This 'mid' is likely the OBU's mid (streamRecv)
                topic = "" # No topic for stream data typically
                # Ensure sendMsg_app is JSON string
                json_sendMsg_app = json.dumps(sendMsg_app, ensure_ascii=False)
                topic_prefixed_message = f"{topic}{json_sendMsg_app}".strip() 
                logger_core_obu.info(f"core_sub2obu: streamRecv, 转发到 APP socket, sid: {data.get('sid')}, data len {len(data.get('data',''))}")
                pub2app_socket.send_string(topic_prefixed_message)
            
            else:
                logger_core_obu.warning(f"[core_sub2obu] 未知消息类型: {message_type}, 原始数据: {data.get('Message_type', 'N/A')}")
        
        except json.JSONDecodeError as e_json:
            logger_core_obu.error(f"[core_sub2obu] JSON 解码错误: {e_json}, 原始消息: {message_str}", exc_info=True)
        except zmq.ContextTerminated:
            logger_core_obu.info("[core_sub2obu] ZMQ 上下文终止，线程退出。")
            break
        except Exception as e:
            logger_core_obu.error(f"[core_sub2obu] 发生错误: {e}", exc_info=True)

def core_echoPub(): # This function is defined but not used in main
    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(f"tcp://*:{config.obu_sub_port}")
    # If used, it should have its own logger, e.g., logger_echo_pub = global_logger.get_logger("core_echo_pub")
    # And then logger_echo_pub.info("Echo publisher started...")
    

def main():
    """启动所有线程"""
    logger_main.info("主函数开始，初始化线程...")
    threads = []
    
    thread_configs = [
        (proxy_send, "ProxySendThread"),
        (proxy_recv, "ProxyRecvThread"),
        (pub2obu_loop, "PubToOBUThread"),
        (core_sub2app, "CoreSubToAppThread"),
        (core_sub2obu, "CoreSubToOBUThread"),
    ]

    for target_func, name in thread_configs:
        t = threading.Thread(target=target_func, daemon=True, name=name)
        threads.append(t)
        t.start()
        logger_main.info(f"线程 '{name}' 已启动。")

    logger_main.info("[Main] 所有核心线程已启动，应用程序运行中...")

    try:
        while True:
            time.sleep(3600) # Sleep for an hour, then check files
            logger_main.info("执行定期旧文件清理...")
            clean_old_files() # Call clean_old_files periodically
    except KeyboardInterrupt:
        logger_main.info("\n检测到 KeyboardInterrupt，开始关闭程序...")
    except Exception as e: # Catch any other exception in main loop
        logger_main.critical(f"主循环发生未捕获异常: {e}", exc_info=True)
    finally:
        logger_main.info("开始关闭程序资源...")
        
        # Attempt to close ZMQ context and sockets if needed, though daemon threads complicate this.
        # For a clean shutdown, threads should have a way to terminate gracefully.
        # context.term() # This is a hard stop, might be too abrupt.

        logger_main.info("正在关闭日志系统...")
        global_logger.shutdown()
        # Messages after this logger shutdown will only go to console if print is used.
        print("\n[✔] 日志系统已关闭。程序退出。")


if __name__ == "__main__":
    # Any initial global configurations can go here
    # For example, setting a different log directory if needed from a command line argument.
    main()