import zmq
import json
import sys
import os
import time
import base64
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import czlconfig

class ICPServer:
    def __init__(self, app_id:int):
        """
        初始化 ICPServer 类，绑定到指定端口。
        :param port: 服务器端口号 (此参数在原始代码中未使用，已移除)
        :param app_id: 应用标识符
        """
        if app_id is None:
            logger.error("app_id 不能为空！请提供一个有效的应用标识符。")
            self.app_id = -1 # 表示无效状态
        else:
            self.app_id = app_id
        
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        try:
            connect_address = f"tcp://{czlconfig.selfip}:{czlconfig.send_sub_port}"
            self.socket.connect(connect_address)
            time.sleep(0.5)
            logger.info(f"Server started, app_id: {self.app_id}, connected to {connect_address}") 
        except AttributeError as e:
            logger.error(f"Config attribute missing (e.g., selfip or send_sub_port): {e}. Server may not function.")
        except zmq.error.ZMQError as e:
            logger.error(f"ZMQ error during server connect: {e}. Server may not function.")
        except Exception as e:
            logger.error(f"An unexpected error occurred during server initialization: {e}")


    def send(self, message: dict):
        logger.info(f"Sending message: {json.dumps(message, ensure_ascii=False)}") 
        try:
            self.socket.send_string(json.dumps(message, ensure_ascii=False))
            #status = self.socket.getsockopt(zmq.EVENTS)
            #logger.info(f"Socket status: {status}")
        except zmq.error.ZMQError as e:
            logger.error(f"Failed to send message via ZMQ: {e}. Message: {json.dumps(message, ensure_ascii=False)}")
        except TypeError as e: # If message is not JSON serializable
            logger.error(f"Failed to serialize message to JSON: {e}. Message structure: {message}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during send: {e}")


    def AppMessage(self, 
                   CapID:int,
                   CapVersion:int,
                   CapConfig:int,
                   act:int,
                   tid:int =0     
                    ):
        """
        构建应用消息
        :param CapID: 能力ID
        :param CapVersion: 能力版本
        :param act: 操作
        """
        if CapID is None or CapVersion is None or act is None or CapConfig is None or tid is None:
            logger.error("AppMessage: CapID, CapVersion, CapConfig, Action 和 tid 不能为空！请提供有效的数据。")
            return
        CapID = CapID & 0xFFFF
        CapVersion = CapVersion & 0xF
        CapConfig = CapConfig & 0xFFF
        message = {
            "mid":czlconfig.appReg,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "capId": CapID,
                "capVersion": CapVersion,
                "capConfig": CapConfig,
                "act": act
            }
        }
        self.send(message)
    
    def brocastPub(self,
                   tid:int =0, 
                   oid:str = "",
                   topic:int = 0,
                   coopMap:bytes = b'',
                   coopMapType:int = 0
                   ):
        """
        广播发布消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param coopMap: 置信图/协作图 (binary data) 携带用于发送的置信图或协作图
        """
        if oid is None or topic is None or coopMap is None or coopMapType is None: # tid 有默认值，一般不会是None
            logger.error("brocastPub: oid, topic, coopMap 和 coopMapType 不能为空！请提供有效的数据。")
            return
        
        coopMap_str = ""
        try:
            coopMap_str = base64.b64encode(coopMap).decode('utf-8')
        except TypeError:
            logger.error(f"brocastPub: coopMap must be bytes-like for Base64 encoding, got {type(coopMap)}.")
            return
        except Exception as e:
            logger.error(f"brocastPub: Error during Base64 encoding of coopMap: {e}")
            return
        
        message = {
            "mid":czlconfig.boardCastPub,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "topic": topic,
                "coopMap": coopMap_str,
                "coopMapType": coopMapType
            }
        }
        self.send(message)
    
    def brocastSub(self,
                   tid:int =0,
                   oid:str = "",
                   topic:int = 0,
                   context:str = "",
                   coopMap:bytes = b'',
                   coopMapType:int = 0,
                   bearCap:int = 0
                   ):
        """
        广播订购消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图 (binary data) 携带用于发送的置信图或协作图
        :param bearcap: 承载能力描述	1：要求携带用于描述自身承载能力的信息
        """
        if oid is None or topic is None or context is None or coopMap is None or bearCap is None or coopMapType is None:
            logger.error("brocastSub: oid, topic, context, coopMap, coopMapType 和 bearCap 不能为空！请提供有效的数据。") 
            return

        coopMap_str = ""
        try:
            coopMap_str = base64.b64encode(coopMap).decode('utf-8')
        except TypeError:
            logger.error(f"brocastSub: coopMap must be bytes-like for Base64 encoding, got {type(coopMap)}.")
            return
        except Exception as e:
            logger.error(f"brocastSub: Error during Base64 encoding of coopMap: {e}")
            return

        message = {
            "mid":czlconfig.boardCastSub,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "topic": topic,
                "context": context,
                "coopMap": coopMap_str,
                "coopMapType": coopMapType,
                "bearCap": bearCap
            }
        }
        self.send(message)
    
    def brocastSubnty(self,
                     tid:int =0,
                     oid:str = "",
                     did:str = "",
                     topic:int = 0,
                     context:str = "",
                     coopMap:bytes = b'', 
                     coopMapType:int = 0,
                     bearCap:int = 0
                     ):
        """
        广播订购通知消息 (brocastSubNotify seems to be the intended name based on config.boardCastSubNotify)
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图 (binary data) 携带用于发送的置信图或协作图
        :param bearcap: 承载能力描述	1：要求携带用于描述自身承载能力的信息
        """
        if oid is None or did is None or topic is None or context is None or coopMap is None or bearCap is None or coopMapType is None:
            logger.error("brocastSubnty: oid, did, topic, context, coopMap, coopMapType 和 bearCap 不能为空！请提供有效的数据。") 
            return

        coopMap_str = ""
        try:
            coopMap_str = base64.b64encode(coopMap).decode('utf-8')
        except TypeError:
            logger.error(f"brocastSubnty: coopMap must be bytes-like for Base64 encoding, got {type(coopMap)}.")
            return
        except Exception as e:
            logger.error(f"brocastSubnty: Error during Base64 encoding of coopMap: {e}")
            return

        message = {
            "mid":czlconfig.boardCastSubNotify,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "did": did,
                "topic": topic,
                "context": context,
                "coopMap": coopMap_str, 
                "coopMapType": coopMapType,
                "bearCap": bearCap
            }
        }
        self.send(message)      
        
    def subMessage(self,
                   tid:int =0,
                   oid:str = "",
                   did:list[str] = [], 
                   topic:int = 0,
                   act:int = 0,
                   context:str = "",
                   coopMap:bytes = b'',
                   coopMapType:int = 0,
                   bearInfo:int = 0
                   ):
        """
        订购消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图 (binary data) 携带用于发送的置信图或协作图
        :param bearInfo: 承载地址描述	1：要求携带用于描述自身承载地址的信息
        """
        if oid is None or did is None or topic is None or context is None or coopMap is None or bearInfo is None or coopMapType is None or act is None:
            logger.error("subMessage: oid, did, topic, act, context, coopMap, coopMapType 和 bearInfo 不能为空！请提供有效的数据。") 
            return

        coopMap_str = ""
        try:
            coopMap_str = base64.b64encode(coopMap).decode('utf-8')
        except TypeError:
            logger.error(f"subMessage: coopMap must be bytes-like for Base64 encoding, got {type(coopMap)}.")
            return
        except Exception as e:
            logger.error(f"subMessage: Error during Base64 encoding of coopMap: {e}")
            return
        
        message = {
            "mid":czlconfig.subScribe,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "did": did,
                "topic": topic,
                "act": act,
                "context": context,
                "coopMap": coopMap_str, 
                "coopMapType": coopMapType,
                "bearinfo": bearInfo 
            }
        }
        self.send(message)
        
    def notifyMessage(self,
                      tid:int =0,
                      oid:str = "",
                      did:str = "",
                      topic:int = 0,
                      act:int = 0,
                      context:str = "",
                      coopMap:bytes = b'',
                      coopMapType:int = 0,
                      bearCap:int = 0
                      ):
        """
        通知消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图 (binary data) 携带用于发送的置信图或协作图
        :param bearCap: 承载能力描述	1：要求携带用于描述自身承载能力的信息
        """
        if oid is None or did is None or topic is None or context is None or coopMap is None or bearCap is None or coopMapType is None or act is None:
            logger.error("notifyMessage: oid, did, topic, act, context, coopMap, coopMapType 和 bearCap 不能为空！请提供有效的数据。") 
            return

        coopMap_str = ""
        try:
            coopMap_str = base64.b64encode(coopMap).decode('utf-8')
        except TypeError:
            logger.error(f"notifyMessage: coopMap must be bytes-like for Base64 encoding, got {type(coopMap)}.")
            return
        except Exception as e:
            logger.error(f"notifyMessage: Error during Base64 encoding of coopMap: {e}")
            return

        message = {
            "mid":czlconfig.notify,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{ 
                "oid": oid,
                "did": did,
                "topic": topic,
                "act": act,
                "context": context,
                "coopMap": coopMap_str,
                "coopMapType": coopMapType,
                "bearCap": bearCap
            }
        }
        self.send(message)
    
    def streamSendreq(self,
                      did:str = "",
                      context:str = "",
                      rl:int =1,
                      pt:int = 0
                      ):
        """
        流发送请求
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param rl: 流数据的质量保证	雷达数据等默认需要 RL=1
        :param pt: 数据类型	请求数据类型
        """
        if did is None or context is None or rl is None or pt is None: 
            logger.error("streamSendreq: did, context, rl 和 pt 不能为空！请提供有效的数据。") 
            return
        message = {
            "mid":czlconfig.streamSendreq,
            "app_id": self.app_id,
            "msg":{
                "did": did,
                "context": context,
                "rl": rl,
                "pt": pt
            }
        }
        self.send(message)

    def streamSend(self,
                   sid:str = "",
                   data:str = "" 
                   ):
        """
        流发送
        :param sid: 流标识	流标识
        :param data: 数据	发送的数据
        """
        if sid is None or data is None:
            logger.error("streamSend: sid 和 data 不能为空！请提供有效的数据。") 
            return
        message = {
            "mid":czlconfig.streamSend,
            "app_id": self.app_id,
            "msg":{
                "sid": sid,
                "data": data 
            }
        }
        self.send(message)

    def streamSendend(self,
                      did:str = "",
                      context:str = "",
                      sid:str = ""
                      ):
        """
        流发送结束
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param sid: 流标识	流标识
        """
        if sid is None or did is None or context is None:
            logger.error("streamSendend: sid, did 和 context 不能为空！请提供有效的数据。") 
            return
        message = {
            "mid":czlconfig.streamSendend,
            "app_id": self.app_id,
            "msg":{
                "did": did,
                "context": context,
                "sid": sid
            }
        }
        self.send(message)
    
    def sendFile(self,
                 did:str = "", 
                 context:str = "",
                 rl:int =1, 
                 pt:int = 0,
                 file:str = ""
                 ):
        """
        发送文件
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param rl: 流数据的质量保证	雷达数据等默认需要 RL=1
        :param pt: 数据类型	请求数据类型
        :param file:文件路径	发送文件的存储路径与文件名
        """
        if did is None or context is None or rl is None or pt is None or file is None:
            logger.error("sendFile: did, context, rl, pt 和 file 不能为空！请提供有效的数据。") 
            return
        message = {
            "mid":czlconfig.sendFile,
            "app_id": self.app_id,
            "msg":{
                "did": did,
                "context": context,
                "rl": rl,
                "pt": pt,
                "file": file
            }
        }
        self.send(message)
        
class ICPClient:
    def __init__(self,topic = ""):
        """
        初始化 ICPClient 类，连接到指定端口。
        """
        self.port = czlconfig.recv_pub_port
        self.ip = czlconfig.selfip
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        try:
            connect_address = f"tcp://{self.ip}:{self.port}"
            self.socket.connect(connect_address)
            self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
            #logger.info(f"Client connected to port {self.port}")
            logger.info(f"Client connected to {connect_address}, subscribed to topic: '{topic}'")
        except AttributeError as e:
            logger.error(f"Config attribute missing (e.g., selfip or recv_pub_port): {e}. Client may not function.")
        except zmq.error.ZMQError as e:
            logger.error(f"ZMQ error during client connect or setsockopt: {e}. Client may not function.")
        except Exception as e:
            logger.error(f"An unexpected error occurred during client initialization: {e}")

        
    def recv_message(self):
        """
        接收消息方法：支持带 topic 和不带 topic 的情况，
        并对消息体中的 'coopMap' 字段进行 Base64 解码。
        """
        raw_message_str = ""
        parsed_message_dict = None
        try:
            raw_message_str = self.socket.recv_string()
        except zmq.error.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                logger.debug("ZMQ recv_string would block (EAGAIN). No message.")
                return None
            logger.error(f"ZMQ error receiving string: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred during recv_string: {e}")
            return None

        try:
            parts = raw_message_str.split(" ", 1)
            json_part_str = ""
            if len(parts) == 2:
                _topic, json_part_str = parts
            else:
                json_part_str = raw_message_str
            
            parsed_message_dict = json.loads(json_part_str)

            # --- 开始解码 coopMap ---
            if isinstance(parsed_message_dict, dict) and "msg" in parsed_message_dict and \
               isinstance(parsed_message_dict["msg"], dict) and "coopmap" in parsed_message_dict["msg"]:
                
                coopMap_str = parsed_message_dict["msg"]["coopmap"]
                if isinstance(coopMap_str, str) and coopMap_str:
                    try:
                        base64_encoded_bytes = coopMap_str.encode('utf-8')
                        original_coopMap_bytes = base64.b64decode(base64_encoded_bytes)
                        
                        parsed_message_dict["msg"]["coopmap"] = original_coopMap_bytes 
                        logger.info(f"Successfully decoded 'coopMap' field.")
                    except base64.binascii.Error as b64_err:
                        logger.error(f"Failed to Base64 decode 'coopMap' string ('{coopMap_str}'): {b64_err}. Keeping as string.")
                    except UnicodeEncodeError as enc_err:
                        logger.error(f"Failed to encode 'coopMap' string to bytes before Base64 decoding: {enc_err}. Keeping as string.")
                    except Exception as e_decode:
                        logger.error(f"An unexpected error occurred during 'coopMap' decoding: {e_decode}. Keeping as string.")
                elif coopMap_str == "":
                     logger.debug("'coopMap' field is an empty string, no decoding needed.")

            return parsed_message_dict
        
        except json.JSONDecodeError:
            logger.error(f"[!] Failed to decode JSON message: {raw_message_str}")
            return None
        except ValueError: 
            logger.error(f"[!] Malformed message structure (expected topic and JSON or just JSON): {raw_message_str}")
            try:
                parsed_message_dict = json.loads(raw_message_str)
                return parsed_message_dict
            except json.JSONDecodeError:
                logger.error(f"[!] Failed to decode message as plain JSON either: {raw_message_str}")
                return None
            except Exception as e_inner:
                logger.error(f"[!] Unexpected error during fallback JSON parsing: {e_inner}. Original message: {raw_message_str}")
                return None
        except Exception as e_outer:
             logger.error(f"[!] Unexpected error processing received message: {e_outer}. Original message: {raw_message_str}")
             return None