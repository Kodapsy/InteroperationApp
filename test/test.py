#from zmq_server import send_zmq
import sys
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
import config
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "module"))
from zmq_server import ICPServer 
def read_json_file():
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data"))
    file_path = os.path.join(data_dir, "tensor_data_small.json")
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.read().strip()  # 直接读取文件内容为字符串
            return data
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None
if __name__ == "__main__":
    server = ICPServer(app_id="test")
    msg = read_json_file()
    while True:
       # try:
            message_type = int(input("Enter message type (1-3) or -1 to exit: ").strip())
            if message_type == -1:
                    print("Publisher shutting down.")
                    break
            if message_type not in [1, 2, 3]:
                print("Invalid message type. Please enter a number between 1 and 5.")
                continue
            topic = input("Enter topic: ").strip()
            if(message_type == 2):
                server.send_sub_message(reliability=0,topic=topic,qos=0, operator=0, source_id=config.source_id, peer_id=config.peer_id)
            elif(message_type == 3):
                server.send_pub_message(reliability=0,data=msg, topic=topic, qos=0, operator=0, source_id=config.source_id, peer_id=config.peer_id)
            else:
                server.send_capsAndmaps_message(capId=0,capVersion=0,capConfig=0,cap_operator=0)
            print(f"Message sent with type {message_type} to topic '{topic}'.")
        #except ValueError:
           # print("Invalid input. Please enter a valid number for message type.")