import zmq
import json

class ICPServer:
    def __init__(self, port=27130, app_id="default_app", reliability=0, length=0):
        """
        初始化 ICPServer 类，绑定到指定端口。
        :param port: 服务器端口号，默认为 27130
        :param app_id: 应用标识符，默认为 "default_app"
        :param reliability: 数据包可靠性，范围 0-3，默认为 0
        :param length: 数据包总长度，范围 0-1500，默认为 0
        :param message_type: ICP 消息类型，范围 0-3，默认为 0
        """
        self.port = port
        self.app_id = app_id
        self.reliability = reliability
        self.length = length
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(f"tcp://*:{self.port}")
        print(f"Server started on port {self.port}")

    def send_message(self, 
                     data="", 
                     caps_list=None, 
                     topic="", 
                     qos=0, 
                     operator="default_operator", 
                     source_id="unknown_source", 
                     peer_id="unknown_peer", 
                     extension=None,
                     message_type=1):
        """
        发送消息方法
        :param data: 消息数据，默认为空字符串
        :param caps_list: 可选，支持的能力列表，默认为 None
        :param topic: 可选，订阅主题，默认为空字符串
        :param qos: 可选，传输级别的 QoS 等级，默认为 0
        :param operator: 可选，操作信息，默认为 "default_operator"
        :param source_id: 可选，源设备 ID，默认为 "unknown_source"
        :param peer_id: 可选，目标设备 ID，默认为 "unknown_peer"
        :param extension: 可选，扩展字段，默认为 None
        """
        message = {
            "ApplicationIdentifier": self.app_id,
            "Reliability": self.reliability,
            "Length": self.length,
            "Message Type": message_type,
            "Data": data,
            "CapsList": caps_list or [],
            "Topic": topic,
            "QoS": qos,
            "Operator": operator,
            "Source Vehicle ID": source_id,
            "Peer Vehicle ID": peer_id,
            "Extension": extension or {}
        }
        # 将字典转换为 JSON 格式并发送
        self.socket.send_json(message)
