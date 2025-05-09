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
from module.logger import global_logger
import queue

os.makedirs(config.data_dir, exist_ok=True)

logger_main = global_logger.get_logger("main")
logger_pub2obu = global_logger.get_logger("pub2obu")
logger_cleaner = global_logger.get_logger("cleaner")
logger_proxy_send = global_logger.get_logger("proxy_send")
logger_proxy_recv = global_logger.get_logger("proxy_recv")
logger_core_app = global_logger.get_logger("core_app")
logger_core_obu = global_logger.get_logger("core_obu")

global_logger.enable_module("main")
global_logger.enable_module("pub2obu")
global_logger.enable_module("cleaner")
global_logger.enable_module("proxy_send")
global_logger.enable_module("proxy_recv")
global_logger.enable_module("core_app")
global_logger.enable_module("core_obu")

MAX_SIZE = 1.4 * 1024
MAX_FILES = 12
caps_instance = CapabilityManager.getInstance()
maps_instance = CollaborationGraphManager.getInstance()
SM_instance = SessionManager.getInstance()
context = zmq.Context()
pub2obu_queue = queue.Queue()

def pub2obu_loop():
    pub_socket = context.socket(zmq.PUB)
    try:
        pub_socket.bind(f"tcp://*:{config.obu_sub_port}")
        logger_pub2obu.info(f"[pub2obu_loop] 启动，绑定到 tcp://*:{config.obu_sub_port}，等待消息发送到 OBU...")
    except zmq.ZMQError as e_bind:
        logger_pub2obu.critical(f"[pub2obu_loop] 绑定端口 {config.obu_sub_port} 失败: {e_bind}，线程退出。", exc_info=True)
        return

    while True:
        msg = None # Initialize msg to None for logging in case of queue.Empty
        try:
            msg = pub2obu_queue.get(timeout=1)
            logger_pub2obu.info(f"[pub2obu_loop] 准备发送消息: {str(msg)[:200]}")
            pub_socket.send_json(msg)
            logger_pub2obu.debug(f"[pub2obu_loop] 消息发送成功: {str(msg)[:200]}")
        except queue.Empty:
            pass
        except TypeError as e_type:
            logger_pub2obu.error(f"[!] pub2obu_loop 发送失败 (类型错误，无法JSON序列化): {e_type}, 消息内容: {str(msg)[:200]}", exc_info=True)
        except zmq.ZMQError as e_zmq_send:
             logger_pub2obu.error(f"[!] pub2obu_loop ZMQ 发送失败: {e_zmq_send}, 消息内容: {str(msg)[:200]}", exc_info=True)
        except Exception as e:
            logger_pub2obu.error(f"[!] pub2obu_loop 发生未知发送失败: {e}, 消息内容: {str(msg)[:200]}", exc_info=True)

def clean_old_files():
    logger_cleaner.debug(f"检查旧文件，目录: {config.data_dir}, MAX_FILES: {MAX_FILES}")
    try:
        json_files = sorted(glob.glob(os.path.join(config.data_dir, "*.json")), key=os.path.getctime)
        logger_cleaner.debug(f"找到 {len(json_files)} 个 JSON 文件。")
        while len(json_files) > MAX_FILES:
            oldest_file = json_files.pop(0)
            logger_cleaner.info(f"文件数量超限，准备删除: {oldest_file}")
            try:
                os.remove(oldest_file)
                logger_cleaner.info(f"[✘] Deleted old file: {oldest_file}")
            except Exception as e_remove:
                logger_cleaner.error(f"[!] Error deleting file {oldest_file}: {e_remove}", exc_info=True)
    except Exception as e_glob:
        logger_cleaner.error(f"[!] Error accessing or sorting files in {config.data_dir}: {e_glob}", exc_info=True)


def proxy_send():
    corexsub = None
    relayxpub = None
    try:
        corexsub = context.socket(zmq.XSUB)
        corexsub.bind(f"tcp://*:{config.send_sub_port}")
        relayxpub = context.socket(zmq.XPUB)
        relayxpub.bind(f"tcp://*:{config.send_pub_port}")
        logger_proxy_send.info(f"[proxy_send] 代理启动，XSUB on *:{config.send_sub_port}, XPUB on *:{config.send_pub_port}")
        zmq.proxy(corexsub, relayxpub)
    except zmq.ContextTerminated:
        logger_proxy_send.info("[proxy_send] ZMQ 上下文已终止，代理关闭。")
    except zmq.ZMQError as e_zmq:
        logger_proxy_send.error(f"[proxy_send] ZMQ 代理发生错误: {e_zmq}", exc_info=True)
    except Exception as e:
        logger_proxy_send.error(f"[proxy_send] 代理发生未知错误并退出: {e}", exc_info=True)
    finally:
        if corexsub:
            corexsub.close()
        if relayxpub:
            relayxpub.close()
        logger_proxy_send.info("[proxy_send] 代理线程结束。")


def proxy_recv():
    corexpub = None
    relayxsub = None
    try:
        corexpub = context.socket(zmq.XPUB)
        corexpub.bind(f"tcp://*:{config.recv_pub_port}")
        relayxsub = context.socket(zmq.XSUB)
        relayxsub.bind(f"tcp://*:{config.recv_sub_port}")
        logger_proxy_recv.info(f"[proxy_recv] 代理启动，XPUB on *:{config.recv_pub_port}, XSUB on *:{config.recv_sub_port}")
        zmq.proxy(corexpub, relayxsub)
    except zmq.ContextTerminated:
        logger_proxy_recv.info("[proxy_recv] ZMQ 上下文已终止，代理关闭。")
    except zmq.ZMQError as e_zmq:
        logger_proxy_recv.error(f"[proxy_recv] ZMQ 代理发生错误: {e_zmq}", exc_info=True)
    except Exception as e:
        logger_proxy_recv.error(f"[proxy_recv] 代理发生未知错误并退出: {e}", exc_info=True)
    finally:
        if corexpub:
            corexpub.close()
        if relayxsub:
            relayxsub.close()
        logger_proxy_recv.info("[proxy_recv] 代理线程结束。")


def core_sub2app():
    sub_socket = None
    pub2app_socket = None
    try:
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{config.selfip}:{config.send_pub_port}")
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        pub2app_socket = context.socket(zmq.PUB)
        pub2app_socket.connect(f"tcp://{config.selfip}:{config.recv_sub_port}")
        logger_core_app.info(f"[core_sub2app] 线程启动，SUB on {config.selfip}:{config.send_pub_port}, PUB on {config.selfip}:{config.recv_sub_port}")
    except zmq.ZMQError as e_setup:
        logger_core_app.critical(f"[core_sub2app] ZMQ socket 初始化失败: {e_setup}, 线程退出。", exc_info=True)
        if sub_socket: sub_socket.close()
        if pub2app_socket: pub2app_socket.close()
        return

    count = 0
    while True:
        message_str = ""
        try:
            if sub_socket.poll(100):
                message_str = sub_socket.recv_string()
                logger_core_app.debug(f"[core_sub2app] 收到原始消息: {message_str[:200]}...")
                try:
                    message = json.loads(message_str)

                    if not isinstance(message, dict):
                        logger_core_app.error(f"[core_sub2app] 消息不是字典类型，跳过：{message_str[:200]}")
                        continue

                    if "mid" not in message:
                        logger_core_app.error(f"[core_sub2app] 消息缺少 'mid' 字段，跳过：{message_str[:200]}")
                        continue

                    logger_core_app.info(f"[core_sub2app] 收到已解析消息 (count {count}), mid: {message.get('mid')}")
                    count += 1

                    mid = message.get("mid", 0)
                    appid = message.get("app_id", "AppIdNotProvided")

                    if mid == config.appReg:
                        logger_core_app.info(f"core_sub2app: 处理 appReg from appid: {appid}")
                        tid = message["tid"]
                        data = message["msg"]
                        msg_response = { "mid" : mid,"tid" : tid }
                        act = data["act"]
                        logger_core_app.debug(f"core_sub2app: appReg action: {act}")
                        if act == config.appActLogin:
                            flag = caps_instance.putCapability(appid, data["capId"], data["capVersion"], data["capConfig"])
                            msg_response["result"] = config.regack if flag else config.regnack
                        elif act == config.appActLogout:
                            flag = caps_instance.deleteCapability(appid, data["capId"], data["capVersion"], data["capConfig"])
                            msg_response["result"] = config.delack if flag else config.delnack
                        elif act == config.appActopen:
                            flag = caps_instance.updateBroadcast(appid, data["capId"], data["capVersion"], data["capConfig"], True)
                            msg_response["result"] = config.openack if flag else config.opennack
                        elif act == config.appActclose:
                            flag = caps_instance.updateBroadcast(appid, data["capId"], data["capVersion"], data["capConfig"], False)
                            msg_response["result"] = config.closeack if flag else config.closenack
                        else:
                            logger_core_app.error(f"core_sub2app: appReg 未知 action: {act}")
                            msg_response["result"] = "NACK_UNKNOWN_ACTION"
                        logger_core_app.info(f"core_sub2app: appReg 发送响应: {msg_response}")
                        pub2app_socket.send_string(json.dumps(msg_response, ensure_ascii=False))
                    elif mid == config.mapReg:
                        logger_core_app.info(f"core_sub2app: 处理 appReg from appid: {appid}")
                        tid = message["tid"]
                        data = message["msg"]
                        msg_response = { "mid" : mid,"tid" : tid }  
                        capId = data.get("capId")
                        capVersion = data.get("capVersion")
                        capConfig = data.get("capConfig")
                        logger_core_app.debug(f"core_sub2app: mapReg capId: {capId}, capVersion: {capVersion}, capConfig: {capConfig}")
                        devices = maps_instance.getDevices(capId, capVersion, capConfig)
                        msg_response["devices"] = devices
                        logger_core_app.info(f"core_sub2app: mapReg 发送响应: {msg_response}")
                        pub2app_socket.send_string(json.dumps(msg_response, ensure_ascii=False))
                        
                    elif mid == config.boardCastPub:
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
                            "Mid": config.boardCastPub
                        }
                        TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                        sendMsg["Payload"] = TLVm
                        byte_TLV = TLVm.encode("utf-8")
                        sendMsg["PayloadLength"] = len(byte_TLV)
                        logger_core_app.info(f"core_sub2app: boardCastPub, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    elif mid == config.boardCastSub:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastSub")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: boardCastSub 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.error(f"core_sub2app: boardCastSub 会话状态更新失败")
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
                            logger_core_app.error(f"core_sub2app: ContextId 超长，已截断: 原始 {len(context_id)} → 64")
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
                        logger_core_app.info(f"core_sub2app: boardCastSub, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    elif mid == config.boardCastSubNotify:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastSubNotify")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: boardCastSubNotify 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.error(f"core_sub2app: boardCastSubNotify 会话状态更新失败")
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
                        logger_core_app.info(f"core_sub2app: boardCastSubNotify, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    elif mid == config.subScribe:
                        logger_core_app.info(f"core_sub2app: 处理 subScribe")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: subScribe 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.error(f"core_sub2app: subScribe 会话状态更新失败")
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
                        logger_core_app.info(f"core_sub2app: subScribe, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    elif mid == config.notify:
                        logger_core_app.info(f"core_sub2app: 处理 notify")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: notify 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.error(f"core_sub2app: notify 会话状态更新失败")
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
                        logger_core_app.info(f"core_sub2app: notify, topic '{data['topic']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    elif mid == config.streamSendreq:
                        logger_core_app.info(f"core_sub2app: 处理 streamSendreq")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: streamSendreq 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.error(f"core_sub2app: streamSendreq 会话状态更新失败")
                        sendMsg = {}
                        sendMsg["RL"] = data["rl"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["PT"] = data["pt"]
                        sendMsg["context"] = data["context"]
                        sendMsg["mid"] = message["mid"]
                        logger_core_app.info(f"core_sub2app: streamSendreq (mid {message['mid']}), context '{data['context']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    elif mid == config.streamSend:
                        logger_core_app.info(f"core_sub2app: 处理 streamSend")
                        data = message["msg"]
                        sendMsg = {}
                        sendMsg["sid"] = data["sid"]
                        sendMsg["data"] = data["data"]
                        sendMsg["mid"] = message["mid"]
                        logger_core_app.info(f"core_sub2app: streamSend (mid {message['mid']}), sid '{data['sid']}', data len {len(data.get('data',''))}, 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)

                    elif mid == config.streamSendend:
                        logger_core_app.info(f"core_sub2app: 处理 streamSendend")
                        data = message["msg"]
                        smFlag = SM_instance.update_state(message["mid"], data["context"])
                        if smFlag:
                            logger_core_app.debug(f"core_sub2app: streamSendend 会话状态更新成功: {SM_instance.sessions}")
                        else:
                            logger_core_app.error(f"core_sub2app: streamSendend 会话状态更新失败")
                        sendMsg = {}
                        sendMsg["sid"] = data["sid"]
                        sendMsg["did"] = data["did"]
                        sendMsg["context"] = data["context"]
                        sendMsg["mid"] = message["mid"]
                        logger_core_app.info(f"core_sub2app: streamSendend (mid {message['mid']}), sid '{data['sid']}', 放入 pub2obu_queue")
                        pub2obu_queue.put(sendMsg)
                    else:
                        logger_core_app.error(f"core_sub2app: 未处理的 mid: {mid}, 消息内容: {message_str[:200]}")

                except json.JSONDecodeError:
                    logger_core_app.error(f"[core_sub2app] JSON 解码失败，原始消息：{message_str[:500]}", exc_info=False)
                    continue
                except KeyError as e_key:
                    logger_core_app.error(f"[core_sub2app] 处理消息时发生 KeyError: {e_key}, 消息: {message_str[:500]}", exc_info=True)
                    continue
                except Exception as e_inner:
                    logger_core_app.error(f"[core_sub2app] 处理单个消息时发生未知错误: {e_inner}, 消息: {message_str[:500]}", exc_info=True)
                    continue
        except zmq.Again:
            pass
        except zmq.ContextTerminated:
            logger_core_app.info("[core_sub2app] ZMQ 上下文终止，线程退出。")
            break
        except Exception as e_outer:
            logger_core_app.critical(f"[core_sub2app] 核心循环发生严重错误: {e_outer}, 原始消息尝试记录: {message_str[:200]}", exc_info=True)
            break # Exit thread on critical outer loop error
    if sub_socket: sub_socket.close()
    if pub2app_socket: pub2app_socket.close()
    logger_core_app.info("[core_sub2app] 线程结束。")


def core_sub2obu():
    sub_socket = None
    pub2app_socket = None
    try:
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{config.selfip}:{config.obu_pub_port}")
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        pub2app_socket = context.socket(zmq.PUB)
        pub2app_socket.connect(f"tcp://{config.selfip}:{config.recv_sub_port}")
        logger_core_obu.info(f"[core_sub2obu] 线程启动，SUB on {config.selfip}:{config.obu_pub_port}, PUB on {config.selfip}:{config.recv_sub_port}")
    except zmq.ZMQError as e_setup:
        logger_core_obu.critical(f"[core_sub2obu] ZMQ socket 初始化失败: {e_setup}, 线程退出。", exc_info=True)
        if sub_socket: sub_socket.close()
        if pub2app_socket: pub2app_socket.close()
        return

    count = 0
    while True:
        message_str = ""
        try:
            message_str = sub_socket.recv_string()
            logger_core_obu.info(f"[core_sub2obu] 从 OBU 收到消息 (count {count}): {message_str[:200]}...")
            count += 1
            try:
                data = json.loads(message_str)
                message_type = data.get("Message_type")

                if message_type == config.echo_type:
                    logger_core_obu.info(f"core_sub2obu: 处理 echo_type from {data.get('Source Vehicle ID')}")
                    maps_instance.updateMapping(data["Source Vehicle ID"], data["CapsList"])
                    logger_core_obu.debug(f"core_sub2obu: echo_type, 更新了来自 {data.get('Source Vehicle ID')} 的映射")

                elif message_type == config.sub_type or message_type == config.pub_type:
                    logger_core_obu.info(f"core_sub2obu: 处理 sub_type/pub_type, topic: {data.get('Topic')}")
                    try:
                        TLVm = data["Payload"]
                        TLVmsg = module.TLV.TLVEncoderDecoder.decode(TLVm)
                        logger_core_obu.debug(f"core_sub2obu: 解码后的 TLV: {TLVmsg}")

                        context_id_tlv = TLVmsg.get("ContextId")
                        mid_tlv = TLVmsg.get("Mid")
                        if mid_tlv != config.boardCastPub:
                            if mid_tlv not in (config.subScribe, config.notify):
                                SM_instance.update_state(mid_tlv, context_id_tlv)
                                logger_core_obu.debug(f"core_sub2obu: 会话状态更新 (mid {mid_tlv}, context {context_id_tlv})")
                            else:
                                SM_instance.update_state(mid_tlv, context_id_tlv, data.get("OP"))
                                logger_core_obu.debug(f"core_sub2obu: 会话状态更新 (mid {mid_tlv}, context {context_id_tlv}, OP {data.get('OP')})")
                        topic = data["Topic"]
                        msg = {}
                        msg["mid"] = mid_tlv
                        if mid_tlv == config.boardCastPub:
                            msg["msg"] = {}
                            msg["msg"]["oid"] = data["SourceId"]
                            msg["msg"]["topic"] = topic
                            msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                        if mid_tlv == config.boardCastSub:
                            msg["msg"] = {}
                            msg["msg"]["oid"] = data["SourceId"]
                            msg["msg"]["topic"] = topic
                            msg["msg"]["context"] = TLVmsg["ContextId"]
                            msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                            msg["msg"]["bearcap"] = TLVmsg["BearFlag"]
                        if mid_tlv == config.boardCastSubNotify:
                            msg["msg"] = {}
                            msg["msg"]["oid"] = data["SourceId"]
                            msg["msg"]["topic"] = topic
                            msg["msg"]["context"] = TLVmsg["ContextId"]
                            msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                            msg["msg"]["bearcap"] = TLVmsg["BearFlag"]
                        if mid_tlv == config.subScribe:
                            msg["msg"] = {}
                            msg["msg"]["oid"] = data["SourceId"]
                            msg["msg"]["topic"] = topic
                            msg["msg"]["act"] = data["OP"]
                            msg["msg"]["context"] = TLVmsg["ContextId"]
                            msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                            msg["msg"]["bearinfo"] = TLVmsg["BearFlag"]
                        if mid_tlv == config.notify:
                            msg["msg"] = {}
                            msg["msg"]["oid"] = data["SourceId"]
                            msg["msg"]["topic"] = topic
                            msg["msg"]["act"] = data["OP"]
                            msg["msg"]["context"] = TLVmsg["ContextId"]
                            msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                            msg["msg"]["bearcap"] = TLVmsg["BearFlag"]
                        topic_prefixed_message = f"{topic} {json.dumps(msg, ensure_ascii=False)}"
                        logger_core_obu.info(f"core_sub2obu: sub_type/pub_type, 转发到 APP socket, topic: {topic}")
                        pub2app_socket.send_string(topic_prefixed_message)
                    except KeyError as e_key_tlv:
                        logger_core_obu.error(f"[core_sub2obu] sub_type/pub_type 缺少 'Payload' 或TLV内部字段: {e_key_tlv}, 原始消息: {message_str[:500]}", exc_info=True)
                        continue
                    except Exception as e_tlv_decode:
                        logger_core_obu.error(f"[core_sub2obu] sub_type/pub_type TLV 处理错误: {e_tlv_decode}, 原始消息: {message_str[:500]}", exc_info=True)
                        continue

                elif message_type == config.sendreq_type:
                    logger_core_obu.info(f"core_sub2obu: 处理 sendreq_type (stream send ready from OBU)")
                    sendMsg_app = {}
                    sendMsg_app["DestId"] = data.get("did")
                    sendMsg_app["context"] = data.get("context")
                    sendMsg_app["sid"] = data.get("sid")
                    sendMsg_app["mid"] = data.get("mid")
                    logger_core_obu.info(f"core_sub2obu: sendreq_type, 转发到 APP socket: {sendMsg_app}")
                    pub2app_socket.send_string(json.dumps(sendMsg_app, ensure_ascii=False))

                    logger_core_obu.info(f"core_sub2obu: sendreq_type, 准备发送 streamSendrdy (mid {config.streamSendrdy}) 到 OBU")
                    pubMsg_obu = config.pubMsg.copy()
                    pubMsg_obu["RT"] = 0
                    pubMsg_obu["SourceId"] = config.source_id
                    pubMsg_obu["DestId"] = data.get("did")
                    pubMsg_obu["OP"] = 0
                    pubMsg_obu["PayloadType"] = config.type_common
                    pubMsg_obu["EncodeMode"] = config.encodeASN
                    TLVmsg_obu = {
                        "StreamId": data.get("sid"),
                        "ContextId": data.get("context"),
                        "Mid": config.streamSendrdy
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
                    sendMsg_app["data"] = data.get("data")
                    sendMsg_app["mid"] = data.get("mid")
                    topic = ""
                    json_sendMsg_app = json.dumps(sendMsg_app, ensure_ascii=False)
                    topic_prefixed_message = f"{topic}{json_sendMsg_app}".strip()
                    logger_core_obu.info(f"core_sub2obu: streamRecv, 转发到 APP socket, sid: {data.get('sid')}, data len {len(data.get('data',''))}")
                    pub2app_socket.send_string(topic_prefixed_message)

                else:
                    logger_core_obu.error(f"[core_sub2obu] 未知消息类型: {message_type}, 原始数据: {message_str[:200]}")

            except json.JSONDecodeError as e_json:
                logger_core_obu.error(f"[core_sub2obu] JSON 解码错误: {e_json}, 原始消息: {message_str[:500]}", exc_info=False)
                continue
            except KeyError as e_key:
                logger_core_obu.error(f"[core_sub2obu] 处理消息时发生 KeyError: {e_key}, 原始消息: {message_str[:500]}", exc_info=True)
                continue
            except Exception as e_inner:
                logger_core_obu.error(f"[core_sub2obu] 处理来自 OBU 的单个消息时发生未知错误: {e_inner}, 原始消息: {message_str[:500]}", exc_info=True)
                continue
        except zmq.ContextTerminated:
            logger_core_obu.info("[core_sub2obu] ZMQ 上下文终止，线程退出。")
            break
        except Exception as e_outer:
            logger_core_obu.critical(f"[core_sub2obu] 核心循环发生严重错误: {e_outer}, 原始消息尝试记录: {message_str[:200]}", exc_info=True)
            break
    if sub_socket: sub_socket.close()
    if pub2app_socket: pub2app_socket.close()
    logger_core_obu.info("[core_sub2obu] 线程结束。")


def core_echoPub():
    # This function is defined but not used.
    # If it were used, it would need error handling and proper socket closing.
    logger_main.error("[core_echoPub] is defined but not currently used in the main execution path.")
    pub_socket = context.socket(zmq.PUB)
    try:
        pub_socket.bind(f"tcp://*:{config.obu_sub_port}")
    except Exception as e:
        logger_main.error(f"[core_echoPub] Error binding: {e}")
    finally:
        if pub_socket:
            pub_socket.close()


def main():
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
    active_thread_check_interval = 60 # Check active threads every 60 seconds
    file_clean_interval = 3600 # Clean files every hour
    last_file_clean_time = time.time()

    try:
        while True:
            # Periodically check if essential threads are alive
            all_threads_alive = True
            for t in threads:
                if not t.is_alive():
                    logger_main.error(f"线程 '{t.name}' 已停止运行！")
                    all_threads_alive = False
            if not all_threads_alive:
                logger_main.critical("一个或多个核心线程已停止，应用程序可能无法正常工作。正在尝试退出...")
                break # Exit main loop if a thread died

            # Periodic file cleaning
            if time.time() - last_file_clean_time >= file_clean_interval:
                logger_main.info("执行定期旧文件清理...")
                clean_old_files()
                last_file_clean_time = time.time()

            time.sleep(active_thread_check_interval)

    except KeyboardInterrupt:
        logger_main.info("\n检测到 KeyboardInterrupt，开始关闭程序...")
    except Exception as e:
        logger_main.critical(f"主循环发生未捕获异常: {e}", exc_info=True)
    finally:
        logger_main.info("开始关闭程序资源...")
        
        # Graceful shutdown for ZMQ context
        # Threads are daemonic, so they will exit when main exits.
        # However, explicitly terminating context can help free resources sooner.
        # Sockets should ideally be closed by their respective threads.
        if not context.closed:
            logger_main.info("正在终止 ZMQ 上下文...")
            context.term()
            logger_main.info("ZMQ 上下文已终止。")

        logger_main.info("正在关闭日志系统...")
        global_logger.shutdown()
        print("\n[✔] 日志系统已关闭。程序退出。")


if __name__ == "__main__":
    main()