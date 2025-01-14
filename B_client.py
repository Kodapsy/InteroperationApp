import zmq
import time
import sys

def client(client_id):
    context = zmq.Context()

    client_socket = context.socket(zmq.DEALER)
    client_socket.identity = client_id.encode()
    client_socket.connect("tcp://192.168.20.224:55555")

    while True:
        msg = [b"Client-specific message from", client_id.encode()]
        client_socket.send_multipart(msg)
        reply = client_socket.recv_multipart()
        print(f'Get reply:{reply}')
        time.sleep(2)

if __name__ == "__main__":
    client_id = sys.argv[1]
    client(client_id)
