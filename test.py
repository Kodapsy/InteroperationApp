#from zmq_server import send_zmq
from zmq_server import ICPServer 
import json
def read_json_file():
    file_path = "tensor_data_small.json"
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
        command = input("Enter command: ").strip().lower()

        if command == "send":
            server.send_message(data=msg)
            print(server.socket)
            #print(f"Message sent: {json.dumps(msg, indent=4)}")

        elif command == "exit":
            print("Publisher shutting down.")
            break
        else:
            print("Invalid command. Type 'send' to broadcast or 'exit' to quit.")