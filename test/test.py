#from zmq_server import send_zmq
import sys
import os
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
    server = ICPServer()
    msg = read_json_file()
    while True:
        try:
            message_type = int(input("Enter message type (1-3) or -1 to exit: ").strip())
            if message_type == -1:
                    print("Publisher shutting down.")
                    break
            if message_type not in [1, 2, 3]:
                print("Invalid message type. Please enter a number between 1 and 3.")
                continue
            topic = input("Enter topic: ").strip()
            server.send_message(data=msg, topic=topic,message_type=message_type)
            print(f"Message sent with type {message_type} to topic '{topic}'.")
        except ValueError:
            print("Invalid input. Please enter a valid number for message type.")