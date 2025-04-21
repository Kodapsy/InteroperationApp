import zmq
import json
import sys
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import config
class ICPServer:
    def __init__(self, app_id:int):
        """
        初始化 ICPServer 类，绑定到指定端口。
        :param port: 服务器端口号
        :param app_id: 应用标识符
        """
        if app_id is None:
            raise ValueError("app_id 不能为空！请提供一个有效的应用标识符。")
        self.app_id = app_id
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(f"tcp://{config.selfip}:{config.send_sub_port}")
        print(f"Server started")
    def send(self, message: dict):
        print(f"Sending message: {message}")
        self.socket.send_string(json.dumps(message, ensure_ascii=False))
        #status = self.socket.getsockopt(zmq.EVENTS)
        #print(f"Socket status: {status}")
    def AppMessage(self, 
                   CapID:int,
                   CapVersion:int,
                   CapConfig:int,
                   act:int,
                   tid:0       
                    ):
        """
        构建应用消息
        :param CapID: 能力ID
        :param CapVersion: 能力版本
        :param act: 操作
        """
        if CapID is None or CapVersion is None or act is None or CapConfig is None or tid is None:
            raise ValueError("CapID, CapVersion, CapConfig, Action 和 tid 不能为空！请提供有效的数据。")
        CapID = CapID & 0xFFFF
        CapVersion = CapVersion & 0xF
        CapConfig = CapConfig & 0xFFF
        message = {
            "mid":config.appReg,
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
                   tid:0,
                   oid:str,
                   topic:int,
                   coopMap:str,
                   coopMapType:int
                   ):
        """
        广播发布消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param coopMap: 置信图/协作图	携带用于发送的置信图或协作图
        """
        if oid is None or topic is None or coopMap is None or coopMapType is None:
            raise ValueError("oid, topic 和 cooMap 不能为空！请提供有效的数据。")
        message = {
            "mid":config.boardCastPub,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "topic": topic,
                "coopMap": coopMap,
                "coopMapType": coopMapType
            }
        }
        self.send(message)
    
    def brocastSub(self,
                   tid:0,
                   oid:str,
                   topic:int,
                   context:str,
                   coopMap:str,
                   coopMapType:int,
                   bearCap:int
                   ):
        """
        广播订购消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图	携带用于发送的置信图或协作图
        :param bearcap: 承载能力描述	1：要求携带用于描述自身承载能力的信息

        """
        if oid is None or topic is None or context is None or coopMap is None or bearCap is None or coopMapType is None:
            raise ValueError("oid, topic, context, coopMap 和 bearcap 不能为空！请提供有效的数据。")
        message = {
            "mid":config.boardCastSub,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "topic": topic,
                "context": context,
                "coopMap": coopMap,
                "coopMapType": coopMapType,
                "bearCap": bearCap
            }
        }
        self.send(message)
    
    def brocastSubnty(self,
                     tid:0,
                     oid:str,
                     did:str,
                     topic:int,
                     context:str,
                     coopMap:str,
                     coopMapType:int,
                     bearcap:int
                     ):
        """
        广播订购消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图	携带用于发送的置信图或协作图
        :param bearcap: 承载能力描述	1：要求携带用于描述自身承载能力的信息
        """
        if oid is None or did is None or topic is None or context is None or coopMap is None or bearcap is None or coopMapType is None:
            raise ValueError("oid, did, topic, context, coopMap 和 bearcap 不能为空！请提供有效的数据。")
        message = {
            "mid":config.boardCastSubNotify,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "did": did,
                "topic": topic,
                "context": context,
                "coopMap": coopMap,
                "coopMapType": coopMapType,
                "bearcap": bearcap
            }
        }
        self.send(message)      
        
    def subMessage(self,
                   tid:0,
                   oid:str,
                   did:list[str],
                   topic:int,
                   act:int,
                   context:str,
                   coopMap:str,
                   coopMapType:int,
                   bearInfo:int
                   ):
        """
        订购消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图	携带用于发送的置信图或协作图
        :param bearInfo: 承载地址描述	1：要求携带用于描述自身承载地址的信息
        """
        if oid is None or did is None or topic is None or context is None or coopMap is None or bearInfo is None or coopMapType is None or act is None:
            raise ValueError("oid, did, topic, act, context, coopMap 和 bearcap 不能为空！请提供有效的数据。")
        message = {
            "mid":config.subScribe,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{
                "oid": oid,
                "did": did,
                "topic": topic,
                "act": act,
                "context": context,
                "coopMap": coopMap,
                "coopMapType": coopMapType,
                "bearinfo": bearInfo
            }
        }
        self.send(message)
        
    def notifyMessage(self,
                      tid:0,
                      oid:str,
                      did:str,
                      topic:int,
                      act:int,
                      context:str,
                      coopMap:str,
                      coopMapType:int,
                      bearCap:int
                      ):
        """
        通知消息
        :param tid: 事物标识（transaction id）	应用与控制层之间的请求与应答消息的关联，随机初始化并自增，保证唯一
        :param oid: 源端标识	广播SUB的源端节点标识（仅通信控制->应用接口携带）
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param topic: 能力标识	广播订购的topic
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param coopMap: 置信图/协作图	携带用于发送的置信图或协作图
        :param bearCap: 承载能力描述	1：要求携带用于描述自身承载能力的信息
        """
        if oid is None or did is None or topic is None or context is None or coopMap is None or bearCap is None or coopMapType is None or act is None:
            raise ValueError("oid, did, topic, act, context, coopMap 和 bearCap 不能为空！请提供有效的数据。")
        message = {
            "mid":config.notify,
            "app_id": self.app_id,
            "tid": tid,
            "msg":{ 
                "oid": oid,
                "did": did,
                "topic": topic,
                "act": act,
                "context": context,
                "coopMap": coopMap,
                "coopMapType": coopMapType,
                "bearCap": bearCap
            }
        }
        self.send(message)
    
    def streamSendreq(self,
                      did:str,
                      context:str,
                      rl:1,
                      pt:int
                      ):
        """
        流发送请求
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param rl: 流数据的质量保证	雷达数据等默认需要 RL=1
        :param pt: 数据类型	请求数据类型
        """
        if did is None or context is None or rl is None or pt is None:
            raise ValueError("did, context, rl 和 pt 不能为空！请提供有效的数据。")
        message = {
            "mid":config.streamSendreq,
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
                   sid:str,
                   data:str
                   ):
        """
        流发送
        :param sid: 流标识	流标识
        :param data: 数据	发送的数据
        """
        if sid is None or data is None:
            raise ValueError("sid 和 data 不能为空！请提供有效的数据。")
        message = {
            "mid":config.streamSend,
            "app_id": self.app_id,
            "msg":{
                "sid": sid,
                "data": data
            }
        }
        self.send(message)
    def streamSendend(self,
                      did:str,
                      context:str,
                      sid:str
                      ):
        """
        流发送结束
        :param did: 目的端标识	广播SUB的目的端节点标识（仅通信控制->应用接口携带）
        :param context: 会上下文标识	应用创建的用于区分对话的标识
        :param sid: 流标识	流标识
        """
        if sid is None or did is None or context is None:
            raise ValueError("sid, did 和 context 不能为空！请提供有效的数据。")
        message = {
            "mid":config.streamSendend,
            "app_id": self.app_id,
            "msg":{
                "did": did,
                "context": context,
                "sid": sid
            }
        }
        self.send(message)
    
    def sendFile(self,
                 did:int,
                 context:str,
                 rl:1,
                 pt:int,
                 file:str
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
            raise ValueError("did, context, rl, pt 和 file 不能为空！请提供有效的数据。")
        message = {
            "mid":config.sendFile,
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
        self.port = config.recv_pub_port
        self.ip = config.selfip
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(f"tcp://{self.ip}:{self.port}")
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        #print(f"Client connected to port {self.port}")
        
    def recv_message(self):
        """
        接收消息方法：支持带 topic 和不带 topic 的情况
        """
        message = self.socket.recv_string()
        try:
            # 判断是否可能包含 topic（按第一个空格拆分）
            topic, json_part = message.split(" ", 1)
            parsed_message = json.loads(json_part)
            return parsed_message
        except json.JSONDecodeError:
            print(f"[!] Failed to decode message: {message}")
            return None
