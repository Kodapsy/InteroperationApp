import zmq
import json

def broker():
    context = zmq.Context()

    router_socket = context.socket(zmq.ROUTER)
    router_socket.bind("tcp://*:55555")

    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind("tcp://*:27130")

    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect("tcp://192.168.20.224:27170")  
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, '')

    poller = zmq.Poller()
    poller.register(router_socket, zmq.POLLIN)
    poller.register(sub_socket, zmq.POLLIN)

    #client_request_map = {}

    while True:
        events = dict(poller.poll())

        if router_socket in events:
            message = router_socket.recv_multipart()
            client_id = message[0].decode()
            json_message = message[1].decode()
            parsed_message = json.loads(json_message)
            print(f'Received meg from {client_id}')
            #client_request_map[client_id] = parsed_message
            """combined_meg = {
                "client_id": client_id,
                "message":parsed_message
            }"""    
            pub_socket.send_json(parsed_message)

        if sub_socket in events:
            service_response = sub_socket.recv_json()
            #client_id = service_response[0]
            print(f"Received service response: {service_response}")
            #router_socket.send_json(json.dumps(service_response))
            #if client_id in client_request_map:
            #    del client_request_map[client_id]
if __name__ == "__main__":
    broker()