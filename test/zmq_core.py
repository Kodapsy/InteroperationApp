import threading
import zmq
import json
import os
import sys
import glob
import time
sys.path.append("/home/nvidia/mydisk/czl/InteroperationApp")
#sys.path.append("/home/czl/InteroperationApp") # 注释掉本地测试路径
from module.CapabilityManager import CapabilityManager
from module.CollaborationGraphManager import CollaborationGraphManager
import czlconfig
import module.TLV
from module.sessionManager import SessionManager
from module.logger import global_logger
import queue

os.makedirs(czlconfig.data_dir, exist_ok=True)

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
        pub_socket.bind(f"tcp://*:{czlconfig.obu_sub_port}")
        logger_pub2obu.info(f"[pub2obu_loop] 启动，绑定到 tcp://*:{czlconfig.obu_sub_port}，等待消息发送到 OBU...")
    except zmq.ZMQError as e_bind:
        logger_pub2obu.critical(f"[pub2obu_loop] 绑定端口 {czlconfig.obu_sub_port} 失败: {e_bind}，线程退出。", exc_info=True)
        return

    while True:
        msg = None
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
    logger_cleaner.debug(f"检查旧文件，目录: {czlconfig.data_dir}, MAX_FILES: {MAX_FILES}")
    try:
        json_files = sorted(glob.glob(os.path.join(czlconfig.data_dir, "*.json")), key=os.path.getctime)
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
        logger_cleaner.error(f"[!] Error accessing or sorting files in {czlconfig.data_dir}: {e_glob}", exc_info=True)


def proxy_send():
    corexsub = None
    relayxpub = None
    try:
        corexsub = context.socket(zmq.XSUB)
        corexsub.bind(f"tcp://*:{czlconfig.send_sub_port}")
        relayxpub = context.socket(zmq.XPUB)
        relayxpub.bind(f"tcp://*:{czlconfig.send_pub_port}")
        logger_proxy_send.info(f"[proxy_send] 代理启动，XSUB on *:{czlconfig.send_sub_port}, XPUB on *:{czlconfig.send_pub_port}")
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
        corexpub.bind(f"tcp://*:{czlconfig.recv_pub_port}")
        relayxsub = context.socket(zmq.XSUB)
        relayxsub.bind(f"tcp://*:{czlconfig.recv_sub_port}")
        logger_proxy_recv.info(f"[proxy_recv] 代理启动，XPUB on *:{czlconfig.recv_pub_port}, XSUB on *:{czlconfig.recv_sub_port}")
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
        sub_socket.connect(f"tcp://{czlconfig.selfip}:{czlconfig.send_pub_port}")
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        pub2app_socket = context.socket(zmq.PUB)
        pub2app_socket.connect(f"tcp://{czlconfig.selfip}:{czlconfig.recv_sub_port}")
        logger_core_app.info(f"[core_sub2app] 线程启动，SUB on {czlconfig.selfip}:{czlconfig.send_pub_port}, PUB on {czlconfig.selfip}:{czlconfig.recv_sub_port}")
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

                    data = message.get("msg", {})
                    if data is not None and not isinstance(data, dict):
                         logger_core_app.error(f"[core_sub2app] 消息的 'msg' 字段不是字典类型，跳过：{message_str[:200]}")
                         continue

                    logger_core_app.info(f"[core_sub2app] 收到已解析消息 (count {count}), mid: {message.get('mid')}")
                    count += 1

                    mid = message.get("mid", 0)
                    appid = message.get("app_id", "AppIdNotProvided")

                    if mid == czlconfig.appReg:
                        logger_core_app.info(f"core_sub2app: 处理 appReg from appid: {appid}")
                        tid = message.get("tid")
                        msg_response = { "mid" : mid,"tid" : tid }
                        act = data.get("act")
                        logger_core_app.debug(f"core_sub2app: appReg action: {act}")

                        required_keys_appReg = ["capId", "capVersion", "capConfig"]
                        if act in [czlconfig.appActLogin, czlconfig.appActLogout, czlconfig.appActopen, czlconfig.appActclose]:
                             if not all(key in data for key in required_keys_appReg):
                                 logger_core_app.error(f"core_sub2app: appReg 消息缺少必要字段: {required_keys_appReg}", exc_info=True)
                                 msg_response["result"] = "NACK_MISSING_FIELDS"
                             elif tid is None:
                                 logger_core_app.error(f"core_sub2app: appReg 消息缺少 'tid' 字段", exc_info=True)
                                 msg_response["result"] = "NACK_MISSING_TID"
                             else:
                                try:
                                    cap_id = data["capId"]
                                    cap_version = data["capVersion"]
                                    cap_config = data["capConfig"]

                                    flag = False
                                    if act == czlconfig.appActLogin:
                                        flag = caps_instance.putCapability(appid, cap_id, cap_version, cap_config)
                                        msg_response["result"] = czlconfig.regack if flag else czlconfig.regnack
                                    elif act == czlconfig.appActLogout:
                                        flag = caps_instance.deleteCapability(appid, cap_id, cap_version, cap_config)
                                        msg_response["result"] = czlconfig.delack if flag else czlconfig.delnack
                                    elif act == czlconfig.appActopen:
                                        flag = caps_instance.updateBroadcast(appid, cap_id, cap_version, cap_config, True)
                                        msg_response["result"] = czlconfig.openack if flag else czlconfig.opennack
                                    elif act == czlconfig.appActclose:
                                        flag = caps_instance.updateBroadcast(appid, cap_id, cap_version, cap_config, False)
                                        msg_response["result"] = czlconfig.closeack if flag else czlconfig.closenack

                                    if not flag:
                                        logger_core_app.error(f"core_sub2app: appReg action {act} failed for appid {appid}, cap {cap_id}:{cap_version}:{cap_config}")

                                except Exception as e_cap_mgr:
                                    logger_core_app.error(f"core_sub2app: appReg action {act} Exception in CapabilityManager: {e_cap_mgr}", exc_info=True)
                                    msg_response["result"] = "NACK_EXCEPTION"

                        elif act is None:
                            logger_core_app.error(f"core_sub2app: appReg 消息缺少 'act' 字段", exc_info=True)
                            msg_response["result"] = "NACK_MISSING_ACT"
                        else:
                            logger_core_app.error(f"core_sub2app: appReg 未知 action: {act}")
                            msg_response["result"] = "NACK_UNKNOWN_ACTION"

                        logger_core_app.info(f"core_sub2app: appReg 发送响应: {msg_response}")
                        try:
                            topic = ""
                            json_msg_response = json.dumps(msg_response, ensure_ascii=False)
                            topic_prefixed_message = f"{topic} {json_msg_response}".strip()
                            pub2app_socket.send_string(topic_prefixed_message)
                        except Exception as e_send_appreg:
                            logger_core_app.error(f"core_sub2app: Failed to send appReg response: {e_send_appreg}", exc_info=True)


                    elif mid == czlconfig.mapReg:
                        logger_core_app.info(f"core_sub2app: 处理 mapReg from appid: {appid}")
                        tid = message.get("tid")
                        msg_response = { "mid" : mid,"tid" : tid }

                        required_keys_mapReg = ["capId", "capVersion", "capConfig"]
                        if not all(key in data for key in required_keys_mapReg):
                             logger_core_app.error(f"core_sub2app: mapReg 消息缺少必要字段: {required_keys_mapReg}", exc_info=True)
                             msg_response["devices"] = []
                             msg_response["result"] = "NACK_MISSING_FIELDS"
                        elif tid is None:
                             logger_core_app.error(f"core_sub2app: mapReg 消息缺少 'tid' 字段", exc_info=True)
                             msg_response["devices"] = []
                             msg_response["result"] = "NACK_MISSING_TID"
                        else:
                            try:
                                capId = data["capId"]
                                capVersion = data["capVersion"]
                                capConfig = data["capConfig"]
                                logger_core_app.debug(f"core_sub2app: mapReg capId: {capId}, capVersion: {capVersion}, capConfig: {capConfig}")
                                devices = maps_instance.getDevices(capId, capVersion, capConfig)
                                msg_response["devices"] = devices
                                msg_response["result"] = "ACK" # Assuming success if devices list is returned
                            except Exception as e_map_mgr:
                                 logger_core_app.error(f"core_sub2app: mapReg Exception in CollaborationGraphManager: {e_map_mgr}", exc_info=True)
                                 msg_response["devices"] = []
                                 msg_response["result"] = "NACK_EXCEPTION"

                        logger_core_app.info(f"core_sub2app: mapReg 发送响应: {msg_response}")
                        try:
                            topic = ""
                            json_msg_response = json.dumps(msg_response, ensure_ascii=False)
                            topic_prefixed_message = f"{topic} {json_msg_response}".strip()
                            pub2app_socket.send_string(topic_prefixed_message)
                        except Exception as e_send_mapreg:
                            logger_core_app.error(f"core_sub2app: Failed to send mapReg response: {e_send_mapreg}", exc_info=True)


                    elif mid == czlconfig.boardCastPub:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastPub")

                        required_keys_pub = ["oid", "topic", "coopMapType", "coopMap"]
                        if not all(key in data for key in required_keys_pub):
                            logger_core_app.error(f"core_sub2app: boardCastPub 消息缺少必要字段: {required_keys_pub}", exc_info=True)
                            continue

                        sendMsg = czlconfig.pubMsg.copy()
                        sendMsg["RT"] = 0
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = ""
                        sendMsg["OP"] = 0
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = czlconfig.type_common
                        sendMsg["EncodeMode"] = czlconfig.encodeASN

                        TLVmsg = {
                            "CommonDataType": data["coopMapType"],
                            "CommonData": data["coopMap"],
                            "Mid": czlconfig.boardCastPub
                        }

                        try:
                            TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                            sendMsg["Payload"] = TLVm
                            byte_TLV = TLVm.encode("utf-8")
                            sendMsg["PayloadLength"] = len(byte_TLV)

                            logger_core_app.info(f"core_sub2app: boardCastPub, topic '{data['topic']}', PayloadLength {sendMsg['PayloadLength']}, 放入 pub2obu_queue")
                            pub2obu_queue.put(sendMsg)

                        except Exception as e_encode:
                            logger_core_app.error(f"core_sub2app: boardCastPub TLV 编码失败: {e_encode}, 原始数据: {TLVmsg}", exc_info=True)
                            # boardCastPub is not session-based, no ACK back to app on encode failure


                    elif mid == czlconfig.boardCastSub:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastSub")

                        required_keys_sub = ["oid", "topic", "coopMapType", "coopMap", "context", "bearCap"]
                        if not all(key in data for key in required_keys_sub):
                            logger_core_app.error(f"core_sub2app: boardCastSub 消息缺少必要字段: {required_keys_sub}", exc_info=True)
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "Missing required fields"}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            logger_core_app.info(f"core_sub2app: boardCastSub 缺少字段，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                            try:
                                pub2app_socket.send_string(topic_prefixed_message)
                            except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (boardCastSub 缺少字段): {e_send_ack}", exc_info=True)
                            continue
                        sendMsg = czlconfig.subMsg.copy()
                        sendMsg["RT"] = 0
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = ""
                        sendMsg["OP"] = 0
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = czlconfig.type_common
                        sendMsg["EncodeMode"] = czlconfig.encodeASN

                        TLVmsg = {
                            "CommonDataType": data["coopMapType"],
                            "CommonData": data["coopMap"],
                            "BearFlag": 1 if data["bearCap"] == 1 else 0,
                            "ContextId": data["context"],
                            "Mid": czlconfig.boardCastSub
                        }

                        try:
                            TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                            sendMsg["Payload"] = TLVm
                            byte_TLV = TLVm.encode("utf-8")
                            sendMsg["PayloadLength"] = len(byte_TLV)

                            logger_core_app.info(f"core_sub2app: boardCastSub, topic '{data['topic']}', PayloadLength {sendMsg['PayloadLength']}, 放入 pub2obu_queue")
                            pub2obu_queue.put(sendMsg)

                            smFlag = SM_instance.update_state(message["mid"], data["context"])
                            if smFlag:
                                logger_core_app.debug(f"core_sub2app: boardCastSub 会话状态更新成功: {SM_instance.sessions}")
                            else:
                                logger_core_app.error(f"core_sub2app: boardCastSub 会话状态更新失败")
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 200}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            pub2app_socket.send_string(topic_prefixed_message)
                            logger_core_app.info(f"core_sub2app:发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")    
                            

                        except Exception as e_encode:
                            logger_core_app.error(f"core_sub2app: boardCastSub TLV 编码失败: {e_encode}, 原始数据: {TLVmsg}", exc_info=True)
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "TLV encode failed"}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            logger_core_app.info(f"core_sub2app: boardCastSub TLV 编码失败，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                            try:
                                pub2app_socket.send_string(topic_prefixed_message)
                            except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (boardCastSub TLV encode failed): {e_send_ack}", exc_info=True)


                    elif mid == czlconfig.boardCastSubNotify:
                        logger_core_app.info(f"core_sub2app: 处理 boardCastSubNotify")

                        required_keys_notify = ["oid", "did", "topic", "coopMapType", "coopMap", "context", "bearCap"]
                        if not all(key in data for key in required_keys_notify):
                             logger_core_app.error(f"core_sub2app: boardCastSubNotify 消息缺少必要字段: {required_keys_notify}", exc_info=True)
                             ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "Missing required fields"}
                             topic_ack = "ACK"
                             json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                             topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                             logger_core_app.info(f"core_sub2app: boardCastSubNotify 缺少字段，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                             try:
                                 pub2app_socket.send_string(topic_prefixed_message)
                             except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (boardCastSubNotify 缺少字段): {e_send_ack}", exc_info=True)
                             continue

                        sendMsg = czlconfig.pubMsg.copy()
                        sendMsg["RT"] = 1
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["OP"] = 0
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = czlconfig.type_common
                        sendMsg["EncodeMode"] = czlconfig.encodeASN

                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":1 if data["bearCap"] == 1 else 0,
                            "ContextId":data["context"],
                            "Mid":czlconfig.boardCastSub
                        }

                        try:
                            TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                            sendMsg["Payload"] = TLVm
                            byte_TLV = TLVm.encode("utf-8")
                            sendMsg["PayloadLength"] = len(byte_TLV)

                            logger_core_app.info(f"core_sub2app: boardCastSubNotify, topic '{data['topic']}', PayloadLength {sendMsg['PayloadLength']}, 放入 pub2obu_queue")
                            pub2obu_queue.put(sendMsg)

                            smFlag = SM_instance.update_state(message["mid"], data["context"])
                            if smFlag:
                                logger_core_app.debug(f"core_sub2app: boardCastSubNotify 会话状态更新成功: {SM_instance.sessions}")
                            else:
                                logger_core_app.error(f"core_sub2app: boardCastSubNotify 会话状态更新失败")
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 200}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            pub2app_socket.send_string(topic_prefixed_message)
                            logger_core_app.info(f"core_sub2app:发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")  

                        except Exception as e_encode:
                            logger_core_app.error(f"core_sub2app: boardCastSubNotify TLV 编码失败: {e_encode}, 原始数据: {TLVmsg}", exc_info=True)
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "TLV encode failed"}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            logger_core_app.info(f"core_sub2app: boardCastSubNotify TLV 编码失败，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                            try:
                                pub2app_socket.send_string(topic_prefixed_message)
                            except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (boardCastSubNotify TLV encode failed): {e_send_ack}", exc_info=True)


                    elif mid == czlconfig.subScribe:
                        logger_core_app.info(f"core_sub2app: 处理 subScribe")

                        required_keys_sub = ["oid", "did", "topic", "coopMapType", "coopMap", "context", "bearinfo", "act"]
                        if not all(key in data for key in required_keys_sub):
                             logger_core_app.error(f"core_sub2app: subScribe 消息缺少必要字段: {required_keys_sub}", exc_info=True)
                             ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "Missing required fields"}
                             topic_ack = "ACK"
                             json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                             topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                             logger_core_app.info(f"core_sub2app: subScribe 缺少字段，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                             try:
                                 pub2app_socket.send_string(topic_prefixed_message)
                             except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (subScribe 缺少字段): {e_send_ack}", exc_info=True)
                             continue

                        sendMsg = czlconfig.subMsg.copy()
                        sendMsg["RT"] = 1
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["OP"] = data["act"]
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = czlconfig.type_common
                        sendMsg["EncodeMode"] = czlconfig.encodeASN

                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":2 if data["bearinfo"] == 1 else 0,
                            "ContextId":data["context"],
                            "Mid":czlconfig.subScribe
                        }

                        try:
                            TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                            sendMsg["Payload"] = TLVm
                            byte_TLV = TLVm.encode("utf-8")
                            sendMsg["PayloadLength"] = len(byte_TLV)

                            logger_core_app.info(f"core_sub2app: subScribe, topic '{data['topic']}', PayloadLength {sendMsg['PayloadLength']}, 放入 pub2obu_queue")
                            pub2obu_queue.put(sendMsg)

                            smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                            if smFlag:
                                logger_core_app.debug(f"core_sub2app: subScribe 会话状态更新成功: {SM_instance.sessions}")
                            else:
                                logger_core_app.error(f"core_sub2app: subScribe 会话状态更新失败")
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 200}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            pub2app_socket.send_string(topic_prefixed_message)
                            logger_core_app.info(f"core_sub2app:发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")  
                        except Exception as e_encode:
                            logger_core_app.error(f"core_sub2app: subScribe TLV 编码失败: {e_encode}, 原始数据: {TLVmsg}", exc_info=True)
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "TLV encode failed"}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            logger_core_app.info(f"core_sub2app: subScribe TLV 编码失败，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                            try:
                                pub2app_socket.send_string(topic_prefixed_message)
                            except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (subScribe TLV encode failed): {e_send_ack}", exc_info=True)


                    elif mid == czlconfig.notify:
                        logger_core_app.info(f"core_sub2app: 处理 notify")

                        required_keys_notify = ["oid", "did", "topic", "coopMapType", "coopMap", "context", "bearCap", "act"]
                        if not all(key in data for key in required_keys_notify):
                             logger_core_app.error(f"core_sub2app: notify 消息缺少必要字段: {required_keys_notify}", exc_info=True)
                             ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "Missing required fields"}
                             topic_ack = "ACK"
                             json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                             topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                             logger_core_app.info(f"core_sub2app: notify 缺少字段，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                             try:
                                 pub2app_socket.send_string(topic_prefixed_message)
                             except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (notify 缺少字段): {e_send_ack}", exc_info=True)
                             continue

                        sendMsg = czlconfig.pubMsg.copy()
                        sendMsg["RT"] = 1
                        sendMsg["SourceId"] = data["oid"]
                        sendMsg["DestId"] = data["did"]
                        sendMsg["OP"] = data["act"]
                        sendMsg["Topic"] = data["topic"]
                        sendMsg["PayloadType"] = czlconfig.type_common
                        sendMsg["EncodeMode"] = czlconfig.encodeASN

                        TLVmsg = {
                            "CommonDataType":data["coopMapType"],
                            "CommonData":data["coopMap"],
                            "BearFlag":1 if data["bearCap"] == 1 else 0,
                            "ContextId":data["context"],
                            "Mid":czlconfig.notify
                        }

                        try:
                            TLVm = module.TLV.TLVEncoderDecoder.encode(TLVmsg)
                            sendMsg["Payload"] = TLVm
                            byte_TLV = TLVm.encode("utf-8")
                            sendMsg["PayloadLength"] = len(byte_TLV)

                            logger_core_app.info(f"core_sub2app: notify, topic '{data['topic']}', PayloadLength {sendMsg['PayloadLength']}, 放入 pub2obu_queue")
                            pub2obu_queue.put(sendMsg)

                            smFlag = SM_instance.update_state(message["mid"], data["context"], data["act"])
                            if smFlag:
                                logger_core_app.debug(f"core_sub2app: notify 会话状态更新成功: {SM_instance.sessions}")
                            else:
                                logger_core_app.error(f"core_sub2app: notify 会话状态更新失败")
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 200}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            pub2app_socket.send_string(topic_prefixed_message)
                            logger_core_app.info(f"core_sub2app:发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")

                        except Exception as e_encode:
                            logger_core_app.error(f"core_sub2app: notify TLV 编码失败: {e_encode}, 原始数据: {TLVmsg}", exc_info=True)
                            ACKMsg = {"mid": czlconfig.Ack, "context": data.get("context"), "result": 400, "error": "TLV encode failed"}
                            topic_ack = "ACK"
                            json_ACKMsg = json.dumps(ACKMsg, ensure_ascii=False)
                            topic_prefixed_message = f"{topic_ack} {json_ACKMsg}".strip()
                            logger_core_app.info(f"core_sub2app: notify TLV 编码失败，发送 ACK 到 app topic '{topic_ack}': {json_ACKMsg}")
                            try:
                                pub2app_socket.send_string(topic_prefixed_message)
                            except Exception as e_send_ack:
                                logger_core_app.error(f"core_sub2app: 发送 ACK 失败 (notify TLV encode failed): {e_send_ack}", exc_info=True)


                    elif mid == czlconfig.streamSendreq:
                        logger_core_app.info(f"core_sub2app: 处理 streamSendreq")

                        required_keys_req = ["rl", "did", "pt", "context"]
                        if not all(key in data for key in required_keys_req):
                             logger_core_app.error(f"core_sub2app: streamSendreq 消息缺少必要字段: {required_keys_req}", exc_info=True)
                             continue

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
                        try:
                            pub2obu_queue.put(sendMsg)
                        except Exception as e_queue:
                             logger_core_app.error(f"core_sub2app: 放入 streamSendreq 消息队列失败: {e_queue}, msg: {sendMsg}", exc_info=True)


                    elif mid == czlconfig.streamSend:
                        logger_core_app.info(f"core_sub2app: 处理 streamSend")

                        required_keys_send = ["sid", "data"]
                        if not all(key in data for key in required_keys_send):
                             logger_core_app.error(f"core_sub2app: streamSend 消息缺少必要字段: {required_keys_send}", exc_info=True)
                             continue

                        sendMsg = {} 
                        sendMsg["sid"] = data["sid"]
                        sendMsg["data"] = data["data"]
                        sendMsg["mid"] = message["mid"]

                        logger_core_app.info(f"core_sub2app: streamSend (mid {message['mid']}), sid '{data['sid']}', data len {len(data.get('data',''))}, 放入 pub2obu_queue")
                        try:
                            pub2obu_queue.put(sendMsg)
                        except Exception as e_queue:
                            logger_core_app.error(f"core_sub2app: 放入 streamSend 消息队列失败: {e_queue}, msg sid: {data.get('sid')}", exc_info=True)


                    elif mid == czlconfig.streamSendend:
                        logger_core_app.info(f"core_sub2app: 处理 streamSendend")

                        required_keys_end = ["sid", "did", "context"]
                        if not all(key in data for key in required_keys_end):
                             logger_core_app.error(f"core_sub2app: streamSendend 消息缺少必要字段: {required_keys_end}", exc_info=True)
                             continue

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
                        try:
                            pub2obu_queue.put(sendMsg)
                        except Exception as e_queue:
                            logger_core_app.error(f"core_sub2app: 放入 streamSendend 消息队列失败: {e_queue}, msg sid: {data.get('sid')}", exc_info=True)


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
            break

    if sub_socket: sub_socket.close()
    if pub2app_socket: pub2app_socket.close()
    logger_core_app.info("[core_sub2app] 线程结束。")


def core_sub2obu():
    sub_socket = None
    pub2app_socket = None
    try:
        sub_socket = context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{czlconfig.selfip}:{czlconfig.obu_pub_port}")
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        pub2app_socket = context.socket(zmq.PUB)
        pub2app_socket.connect(f"tcp://{czlconfig.selfip}:{czlconfig.recv_sub_port}")
        logger_core_obu.info(f"[core_sub2obu] 线程启动，SUB on {czlconfig.selfip}:{czlconfig.obu_pub_port}, PUB on {czlconfig.selfip}:{czlconfig.recv_sub_port}")
    except zmq.ZMQError as e_setup:
        logger_core_obu.critical(f"[core_sub2obu] ZMQ socket 初始化失败: {e_setup}, 线程退出。", exc_info=True)
        if sub_socket: sub_socket.close()
        if pub2app_socket: pub2app_socket.close()
        return

    count = 0
    while True:
        message_str = ""
        try:
            if sub_socket.poll(100):
                message_str = sub_socket.recv_string()
                logger_core_obu.info(f"[core_sub2obu] 从 OBU 收到消息 (count {count}): {message_str[:200]}...")
                count += 1
                try:
                    data = json.loads(message_str)
                    message_type = data.get("Message_type")

                    if message_type is None:
                         logger_core_obu.error(f"[core_sub2obu] 收到消息缺少 'Message_type' 字段, 跳过: {message_str[:200]}")
                         continue

                    if message_type == czlconfig.echo_type:
                        logger_core_obu.info(f"core_sub2obu: 处理 echo_type from {data.get('Source Vehicle ID')}")
                        required_keys_echo = ["Source Vehicle ID", "CapsList"]
                        if not all(key in data for key in required_keys_echo):
                             logger_core_obu.error(f"[core_sub2obu] echo_type 消息缺少必要字段: {required_keys_echo}", exc_info=True)
                             continue

                        maps_instance.updateMapping(data["Source Vehicle ID"], data["CapsList"])
                        logger_core_obu.debug(f"core_sub2obu: echo_type, 更新了来自 {data.get('Source Vehicle ID')} 的映射")

                    elif message_type == czlconfig.sub_type or message_type == czlconfig.pub_type:
                        logger_core_obu.info(f"core_sub2obu: 处理 sub_type/pub_type, topic: {data.get('Topic')}")

                        required_keys_czl_header = ["Topic", "Payload"]
                        if not all(key in data for key in required_keys_czl_header):
                             logger_core_obu.error(f"[core_sub2obu] CZL消息缺少必要字段: {required_keys_czl_header}, 消息类型: {message_type}", exc_info=True)
                             continue

                        try:
                            TLVm = data["Payload"]
                            TLVmsg = module.TLV.TLVEncoderDecoder.decode(TLVm)
                            logger_core_obu.debug(f"core_sub2obu: 解码后的 TLV: {TLVmsg}")

                            mid_tlv = TLVmsg.get("Mid")
                            if mid_tlv is None:
                                logger_core_obu.error(f"[core_sub2obu] TLV payload 缺少 'Mid' 字段, 跳过: {str(TLVmsg)[:200]}")
                                continue

                            if mid_tlv != czlconfig.boardCastPub:
                                context_id_tlv = TLVmsg.get("ContextId")
                                if context_id_tlv: # 仅在 context 存在时更新状态
                                    if mid_tlv in (czlconfig.subScribe, czlconfig.notify):
                                        smFlag = SM_instance.update_state(mid_tlv, context_id_tlv, data.get("OP"))
                                        logger_core_obu.debug(f"core_sub2obu: 会话状态更新 (mid {mid_tlv}, context {context_id_tlv}, OP {data.get('OP')})")
                                    else:
                                        smFlag = SM_instance.update_state(mid_tlv, context_id_tlv)
                                        logger_core_obu.debug(f"core_core_obu: 会话状态更新 (mid {mid_tlv}, context {context_id_tlv})")
                                    if not smFlag:
                                         logger_core_obu.error(f"core_sub2obu: 会话状态更新失败 for mid {mid_tlv}, context {context_id_tlv}")


                            topic = data["Topic"]
                            msg = {}

                            if mid_tlv == czlconfig.boardCastPub:
                                required_keys_tlv_pub = ["CommonData"]
                                if not all(key in TLVmsg for key in required_keys_tlv_pub):
                                     logger_core_obu.error(f"[core_sub2obu] boardCastPub TLV payload 缺少必要字段: {required_keys_tlv_pub}", exc_info=True)
                                     continue
                                msg["mid"] = mid_tlv
                                msg["msg"] = {}
                                msg["msg"]["oid"] = data.get("SourceId", "Unknown")
                                msg["msg"]["topic"] = topic
                                msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                                msg["msg"]["coopmaptype"] = TLVmsg["CommonDataType"]

                            elif mid_tlv == czlconfig.boardCastSub:
                                required_keys_tlv_sub = ["CommonData", "ContextId", "BearFlag"]
                                if not all(key in TLVmsg for key in required_keys_tlv_sub):
                                     logger_core_obu.error(f"[core_sub2obu] boardCastSub TLV payload 缺少必要字段: {required_keys_tlv_sub}", exc_info=True)
                                     continue
                                msg["mid"] = mid_tlv
                                msg["msg"] = {}
                                msg["msg"]["oid"] = data.get("SourceId", "Unknown")
                                msg["msg"]["topic"] = topic
                                msg["msg"]["context"] = TLVmsg["ContextId"]
                                msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                                msg["msg"]["bearcap"] = TLVmsg["BearFlag"]

                            elif mid_tlv == czlconfig.boardCastSubNotify:
                                required_keys_tlv_notify = ["CommonData", "ContextId", "BearFlag"]
                                if not all(key in TLVmsg for key in required_keys_tlv_notify):
                                     logger_core_obu.error(f"[core_sub2obu] boardCastSubNotify TLV payload 缺少必要字段: {required_keys_tlv_notify}", exc_info=True)
                                     continue
                                msg["mid"] = mid_tlv
                                msg["msg"] = {}
                                msg["msg"]["oid"] = data.get("SourceId", "Unknown")
                                msg["msg"]["topic"] = topic
                                msg["msg"]["context"] = TLVmsg["ContextId"]
                                msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                                msg["msg"]["bearcap"] = TLVmsg["BearFlag"]

                            elif mid_tlv == czlconfig.subScribe:

                                required_keys_tlv_sub = ["CommonData", "ContextId", "BearFlag"]
                                if not all(key in TLVmsg for key in required_keys_tlv_sub):
                                     logger_core_obu.error(f"[core_sub2obu] subScribe TLV payload 缺少必要字段: {required_keys_tlv_sub}", exc_info=True)
                                     continue

                                msg["mid"] = mid_tlv
                                msg["msg"] = {}
                                msg["msg"]["oid"] = data.get("SourceId", "Unknown")
                                msg["msg"]["topic"] = topic
                                msg["msg"]["act"] = data.get("OP")
                                msg["msg"]["context"] = TLVmsg["ContextId"]
                                msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                                msg["msg"]["bearinfo"] = TLVmsg["BearFlag"]

                            elif mid_tlv == czlconfig.notify:
                                required_keys_tlv_notify = ["CommonData", "ContextId", "BearFlag"]
                                if not all(key in TLVmsg for key in required_keys_tlv_notify):
                                     logger_core_obu.error(f"[core_sub2obu] notify TLV payload 缺少必要字段: {required_keys_tlv_notify}", exc_info=True)
                                     continue
                                msg["mid"] = mid_tlv
                                msg["msg"] = {}
                                msg["msg"]["oid"] = data.get("SourceId", "Unknown")
                                msg["msg"]["topic"] = topic
                                msg["msg"]["act"] = data.get("OP")
                                msg["msg"]["context"] = TLVmsg["ContextId"]
                                msg["msg"]["coopmap"] = TLVmsg["CommonData"]
                                msg["msg"]["bearcap"] = TLVmsg["BearFlag"]

                            elif mid_tlv == czlconfig.streamSendrdy:
                                logger_core_obu.info(f"core_sub2obu: 处理 streamSendrdy (mid {mid_tlv})")
                                required_keys_tlv_ready = ["StreamId", "ContextId"]
                                if not all(key in TLVmsg for key in required_keys_tlv_ready):
                                     logger_core_obu.error(f"[core_sub2obu] streamSendrdy TLV payload 缺少必要字段: {required_keys_tlv_ready}", exc_info=True)
                                     continue
                                msg["mid"] = mid_tlv
                                msg["sid"] = TLVmsg["StreamId"]
                                msg["context"] = TLVmsg["ContextId"]
                                msg["oid"] = data.get("DestId")

                                logger_core_obu.info(f"core_sub2obu: streamSendrdy, 转发到 APP socket: {msg}")
                                topic_stream = "W"
                                json_msg = json.dumps(msg, ensure_ascii=False)
                                topic_prefixed_message = f"{topic_stream} {json_msg}".strip()
                                pub2app_socket.send_string(topic_prefixed_message)
                                continue 

                            elif mid_tlv == czlconfig.streamSendend:
                                logger_core_obu.info(f"core_sub2obu: 处理 streamSendend ACK (mid {mid_tlv})")
                                required_keys_tlv_end_ack = ["StreamId", "ContextId"]
                                if not all(key in TLVmsg for key in required_keys_tlv_end_ack):
                                     logger_core_obu.error(f"[core_sub2obu] streamSendend TLV payload 缺少必要字段: {required_keys_tlv_end_ack}", exc_info=True)
                                     continue
                                msg["mid"] = mid_tlv
                                msg["sid"] = TLVmsg["StreamId"]
                                msg["context"] = TLVmsg["ContextId"]
                                msg["oid"] = data.get("SourceId")

                                logger_core_obu.info(f"core_sub2obu: streamSendend ACK 从 OBU 收到，转发到 APP socket: {msg}")
                                topic_stream = "W"
                                json_msg = json.dumps(msg, ensure_ascii=False)
                                topic_prefixed_message = f"{topic_stream} {json_msg}".strip()
                                try:
                                    pub2app_socket.send_string(topic_prefixed_message)
                                except Exception as e_send_app:
                                     logger_core_obu.error(f"core_sub2obu: 转发 streamSendend ACK 到 APP socket 失败: {e_send_app}", exc_info=True)
                                continue 

                            if "mid" in msg and "msg" in msg:
                                topic_prefixed_message = f"{topic} {json.dumps(msg, ensure_ascii=False)}"
                                logger_core_obu.info(f"core_sub2obu: {message_type}, topic '{topic}', 转发到 APP socket: {str(msg)[:200]}...")
                                try:
                                    pub2app_socket.send_string(topic_prefixed_message)
                                except Exception as e_send_app:
                                     logger_core_obu.error(f"core_sub2obu: 转发消息到 APP socket 失败: {e_send_app}", exc_info=True)
                            else:
                                logger_core_obu.error(f"core_sub2obu: 收到未处理的 TLV mid {mid_tlv} ({message_type}), 或构建 APP 消息失败。TLVmsg: {str(TLVmsg)[:200]}")


                        except KeyError as e_key_tlv:
                            logger_core_obu.error(f"[core_sub2obu] CZL消息处理时缺少必要字段 (TLV或Header): {e_key_tlv}, 原始消息: {message_str[:500]}", exc_info=True)
                            # 未定义错误响应回 OBU 或 App，仅记录并跳过。
                            continue
                        except Exception as e_tlv_decode:
                            logger_core_obu.error(f"[core_sub2obu] CZL消息 TLV 处理错误或未知TLVMid: {e_tlv_decode}, 原始消息: {message_str[:500]}", exc_info=True)
                            # 未定义错误响应回 OBU 或 App，仅记录并跳过。
                            continue

                    elif message_type == czlconfig.streamRecv:
                        logger_core_obu.info(f"core_sub2obu: 处理 streamRecv")
                        # OBU 发送流数据块，需要转发给对应的 App

                        required_keys_stream_recv = ["sid", "data", "mid"]
                        if not all(key in data for key in required_keys_stream_recv):
                             logger_core_obu.error(f"[core_sub2obu] streamRecv 消息缺少必要字段: {required_keys_stream_recv}", exc_info=True)
                             continue

                        # 构建发送给 APP 的消息
                        sendMsg_app = {}
                        sendMsg_app["sid"] = data["sid"]
                        sendMsg_app["data"] = data["data"]
                        sendMsg_app["mid"] = data["mid"]

                        logger_core_obu.info(f"core_sub2obu: streamRecv, 转发到 APP socket, sid: {data.get('sid')}, data len {len(data.get('data',''))}")
                        # 流数据使用特定 topic (如 "W") 发送给 App
                        topic_stream = "W"
                        json_sendMsg_app = json.dumps(sendMsg_app, ensure_ascii=False)
                        topic_prefixed_message = f"{topic_stream} {json_sendMsg_app}".strip()
                        try:
                            pub2app_socket.send_string(topic_prefixed_message)
                        except Exception as e_send_app:
                             logger_core_obu.error(f"core_sub2obu: 转发 streamRecv 消息到 APP socket 失败: {e_send_app}", exc_info=True)
                    else:
                        logger_core_obu.error(f"[core_sub2obu] 收到未知消息类型从 OBU: {message_type}, 原始数据: {message_str[:200]}")

                except json.JSONDecodeError as e_json:
                    logger_core_obu.error(f"[core_sub2obu] JSON 解码错误: {e_json}, 原始消息: {message_str[:500]}", exc_info=False)
                    continue
                except KeyError as e_key:
                    logger_core_obu.error(f"[core_sub2obu] 处理消息时发生 KeyError: {e_key}, 原始消息: {message_str[:500]}", exc_info=True)
                    continue
                except Exception as e_inner:
                    logger_core_obu.error(f"[core_sub2obu] 处理来自 OBU 的单个消息时发生未知错误: {e_inner}, 原始消息: {message_str[:500]}", exc_info=True)
                    continue

        except zmq.Again:
             pass
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
    # 此函数已定义但目前未使用
    logger_main.error("[core_echoPub] is defined but not currently used in the main execution path.")
    pub_socket = None
    try:
        pub_socket = context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{czlconfig.obu_sub_port}")
        logger_main.error(f"[core_echoPub] Attempted to bind to {czlconfig.obu_sub_port}. This port is likely already used by pub2obu_loop.")
    except zmq.ZMQError as e_bind:
         logger_main.error(f"[core_echoPub] Error binding to port {czlconfig.obu_sub_port}: {e_bind}", exc_info=True)
    except Exception as e:
        logger_main.error(f"[core_echoPub] An unexpected error occurred: {e}", exc_info=True)
    finally:
        if pub_socket and not pub_socket.closed:
            pub_socket.close()
            logger_main.info("[core_echoPub] Socket closed.")


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
    active_thread_check_interval = 60 # 秒
    file_clean_interval = 3600 # 秒
    last_file_clean_time = time.time()

    try:
        while True:
            # 定期检查关键线程是否存活
            active_threads = [t for t in threads if t.is_alive()]
            if len(active_threads) < len(thread_configs):
                 logger_main.critical(f"发现 {len(threads) - len(active_threads)} 个核心线程已停止运行！")
                 for t in threads:
                     if not t.is_alive():
                         logger_main.error(f"停止的线程: '{t.name}'")
                 logger_main.critical("一个或多个核心线程已停止，应用程序可能无法正常工作。正在尝试退出...")
                 break
            threads = active_threads

            # 定期清理旧文件
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

        # 终止 ZMQ 上下文
        if not context.closed:
            logger_main.info("正在终止 ZMQ 上下文...")
            try:
                 # 设置 LINGER 为 0 确保 socket 立即关闭，避免阻塞
                 context.set(zmq.LINGER, 0)
                 context.term()
                 logger_main.info("ZMQ 上下文已终止。")
            except Exception as e_term:
                 logger_main.error(f"终止 ZMQ 上下文时出错: {e_term}", exc_info=True)

        # 短暂等待守护线程退出
        time.sleep(1)

        logger_main.info("正在关闭日志系统...")
        global_logger.shutdown()
        print("\n[✔] 日志系统已关闭。程序退出。")


if __name__ == "__main__":
    main()