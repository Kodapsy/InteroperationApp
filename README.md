# InteroperationApp
The interoperability support system application end for autonomous transportation. This repository implements message distribution based on a broker proxy at the application end and communication between the application end and the protocol stack based on ZMQ.

ZMQ使用方法：
上层应用:
首先需要引用zmq_server #from zmq_server import ICPServer from zmq_server import ICPClient
引用后，需要在程序中构建两个类（以构建两个socket），类似：server = ICPServer(app_id) , client = ICPClient(app_id)

然后使用两个类里的方法来发送接受消息，
如在ICPServer中：
有：
server.send_sub_message(reliability=0,topic=topic,qos=0, operator=0, source_id=config.source_id, peer_id=config.peer_id)
server.send_pub_message(reliability=0,data=msg, topic=topic, qos=0, operator=0, source_id=config.source_id, peer_id=config.peer_id)
server.send_capsAndmaps_message(capId=0,capVersion=0,capConfig=0,cap_operator=0)
注意括号能都是必填参数！，详细参考文档

在ICPClient中：
有：
recv_message(topic:str)
必须携带topic