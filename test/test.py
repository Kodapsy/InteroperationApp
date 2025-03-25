import sys
import os
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)

import config
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(project_root, "module"))

from zmq_server import ICPServer 

def read_json_file():
    """读取 JSON 文件"""
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

    while True:
        try:
            # 输入消息类型
            message_type = int(input("Enter message type (1-3) or -1 to exit: ").strip())
            if message_type == -1:
                print("Publisher shutting down.")
                break
            if message_type not in [1, 2, 3]:
                print("Invalid message type. Please enter a number between 1 and 3.")
                continue
            
            # 用户输入通用参数

            # 发送不同类型的消息
            if message_type == 2:
                topic = input("Enter topic: ").strip()
                reliability = int(input("Enter reliability (0 or 1): ").strip())
                qos = int(input("Enter QoS level (0-2): ").strip())
                operator = int(input("Enter operator: ").strip())
                source_id = input("Enter source ID: ").strip()
                peer_id = input("Enter peer ID: ").strip()
                server.send_sub_message(
                    reliability=reliability, 
                    topic=topic, 
                    qos=qos, 
                    operator=operator, 
                    source_id=source_id, 
                    peer_id=peer_id
                )
                print(f"Message sent with type {message_type} to topic '{topic}'.")
            elif message_type == 3:
                topic = input("Enter topic: ").strip()
                reliability = int(input("Enter reliability (0 or 1): ").strip())
                qos = int(input("Enter QoS level (0-2): ").strip())
                operator = int(input("Enter operator: ").strip())
                source_id = input("Enter source ID: ").strip()
                peer_id = input("Enter peer ID: ").strip()
                msg = read_json_file()
                if msg is None:
                    print("Failed to read JSON data. Aborting send.")
                    continue
                server.send_pub_message(
                    reliability=reliability, 
                    data=msg, 
                    topic=topic, 
                    qos=qos, 
                    operator=operator, 
                    source_id=source_id, 
                    peer_id=peer_id
                )
                print(f"Message sent with type {message_type} to topic '{topic}'.")
            else:
                capId = int(input("Enter capId: ").strip())
                capVersion = int(input("Enter capVersion: ").strip())
                capConfig = int(input("Enter capConfig: ").strip())
                cap_operator = int(input("Enter cap_operator: ").strip())
                if cap_operator == 0:
                    server.send_caps_message(
                        capId=capId, 
                        capVersion=capVersion, 
                        capConfig=capConfig, 
                    )
                elif cap_operator == 1:
                    server.delete_caps_message(
                        capId=capId, 
                        capVersion=capVersion, 
                        capConfig=capConfig, 
                    )
                else:
                    server.get_maps_message(
                        capId=capId, 
                        capVersion=capVersion, 
                        capConfig=capConfig, 
                    )

        except ValueError:
            print("Invalid input. Please enter valid numbers where required.")
