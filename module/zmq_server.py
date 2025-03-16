import zmq
import json
import sys
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import config
class ICPServer:
    def __init__(self, app_id:str):
        """
        初始化 ICPServer 类，绑定到指定端口。
        :param port: 服务器端口号
        :param app_id: 应用标识符
        """
        if not app_id:
            raise ValueError("app_id 不能为空！请提供一个有效的应用标识符。")
        self.app_id = app_id
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.connect(f"tcp://{config.selfip}:{config.send_sub_port}")
        print(f"Server started")

    def send_pub_message(self, 
                    reliability:int,
                    data:str, 
                    qos:int, 
                    operator:int, 
                    topic:str, 
                    source_id:str, 
                    peer_id:str, 
                    extension=None):
        """
        发送消息方法
        :param reliability: 可靠性，默认为 INT
        :param data: 消息数据，默认为空字符串
        :param caps_list: 可选，支持的能力列表，默认为 None
        :param topic: 可选，订阅主题，默认为空字符串
        :param qos: 可选，传输级别的 QoS 等级，默认为 0
        :param operator: 必选，操作，默认为空字符串
        :param source_id: 可选，源车辆 ID，默认为空字符串
        :param peer_id: 可选，对等车辆 ID，默认为空字符串
        :param extension: 可选，扩展字段，默认为 None
        """
        if data is None or reliability is None or operator is None or topic is None or source_id is None or peer_id is None:
            raise ValueError("data, reliability, operator, topic, source_id, peer_id 不能为空！请提供有效的数据。")
        message = {
            "Send Type":0,
            "ApplicationIdentifier": self.app_id,
            "Reliability": reliability,
            "Message Type": 3,
            "Data": data,
            "Topic": topic,
            "Operator": operator,
            "Source Vehicle ID": source_id,
            "Peer Vehicle ID": peer_id,
            "Extension": extension or {}
        }
        # 将字典转换为 JSON 格式并发送
        if qos:
            message["Qos"] = qos
        message_str = json.dumps(message, ensure_ascii=False)
        message["Length"] = len(message_str.encode('utf-8'))
        self.socket.send_json(message)
    def send_sub_message(self, 
                    reliability:int,
                    qos:int, 
                    operator:int, 
                    topic:str, 
                    source_id:str, 
                    peer_id:str, 
                    extension=None):
        """
        发送消息方法
        :param data: 消息数据，默认为空字符串
        :param caps_list: 可选，支持的能力列表，默认为 None
        :param topic: 可选，订阅主题，默认为空字符串
        :param qos: 可选，传输级别的 QoS 等级，默认为 0
        :param operator: 必选，操作，默认为空字符串
        :param source_id: 可选，源车辆 ID，默认为空字符串
        :param peer_id: 可选，对等车辆 ID，默认为空字符串
        :param extension: 可选，扩展字段，默认为 None
        """
        if reliability is None or operator is None or topic is None or source_id is None or peer_id is None:
            raise ValueError("reliability, qos, operator, topic 不能为空！请提供有效的数据。")
        message = {
            "Send Type":0,
            "ApplicationIdentifier": self.app_id,
            "Reliability": reliability,
            "Message Type": 2,
            "Topic": topic,
            "Operator": operator,
            "Qos": qos,
            "Source Vehicle ID": source_id,
            "Peer Vehicle ID": peer_id,
            "Extension": extension or {}
        }
        # 将字典转换为 JSON 格式并发送
        message_str = json.dumps(message, ensure_ascii=False)
        message["Length"] = len(message_str.encode('utf-8'))
        self.socket.send_json(message)
    
    def send_caps_message(self,
                          capId:int,
                          capVersion:int,
                          capConfig:int          
                            ):
        if capId is None or capVersion is None or capConfig is None:
            raise ValueError("capId, capVersion, capConfig ,capOperator 不能为空！请提供有效的数据。")
        message = {
            "Send Type":1,
            "ApplicationIdentifier": self.app_id,
            "Cap ID": capId,
            "Cap Version": capVersion,
            "Cap Configuration": capConfig,
            "Cap Operator": 0
        }
        self.socket.send_json(message)
    def delete_caps_message(self,
                          capId:int,
                          capVersion:int,
                          capConfig:int          
                            ):
        if capId is None or capVersion is None or capConfig is None:
            raise ValueError("capId, capVersion, capConfig ,capOperator 不能为空！请提供有效的数据。")
        message = {
            "Send Type":1,
            "ApplicationIdentifier": self.app_id,
            "Cap ID": capId,
            "Cap Version": capVersion,
            "Cap Configuration": capConfig,
            "Cap Operator": 1
        }
        self.socket.send_json(message)
    def get_maps_message(self,
                          capId:int,
                          capVersion:int,
                          capConfig:int          
                            ):
        if capId is None or capVersion is None or capConfig is None:
            raise ValueError("capId, capVersion, capConfig ,capOperator 不能为空！请提供有效的数据。")
        message = {
            "Send Type":1,
            "ApplicationIdentifier": self.app_id,
            "Cap ID": capId,
            "Cap Version": capVersion,
            "Cap Configuration": capConfig,
            "Cap Operator": 2
        }
        self.socket.send_json(message)

class ICPClient:
    def __init__(self):
        """
        初始化 ICPClient 类，连接到指定端口。
        """
        self.port = config.recv_pub_port
        self.ip = config.selfip
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.connect(f"tcp://{self.ip}:{self.port}")
        #print(f"Client connected to port {self.port}")
        
    def recv_message(self,topic:str):
        """
        接收消息方法
        """
        if not topic:
            raise ValueError("topic 不能为空！请提供有效的数据。")
        self.socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        message = self.socket.recv_string()
        try:
            parsed_message = json.loads(message)
            return parsed_message
        except json.JSONDecodeError:
            print(f"Failed to decode message: {message}")
            return None