import time
import json
import sys

# Ensure the paths are correct for your environment
sys.path.append("/home/nvidia/mydisk/czl/InteroperationApp")
# sys.path.append("/home/czl/InteroperationApp") # Can be removed if covered

from module.zmq_server import ICPServer, ICPClient # Assuming zmq_server.py contains the ICPServer expecting bytes for coopMap
import InteroperationApp.czlconfig as czlconfig # Import the original config module

def main():
    print("🚀 ICPServer 测试程序启动")
    app_id = 0
    server = ICPServer(app_id=app_id)
    tid = 1
    oid = "京A12345"
    did = "津A12345"
    topic = 12345
    act = 1
    context = "dddddddddddddddddddddddddddddddd"
    coopMap_input = "通知的协作图内容" # This will be '通知的协作图内容'
    coopMap_bytes = coopMap_input.encode('utf-8') # This will be b'\xe9\x80\x9a\xe7\x9f\xa5\xe7\x9a\x84\xe5\x8d\x8f\xe4\xbd\x9c\xe5\x9b\xbe\xe5\x86\x85\xe5\xae\xb9'
    coopMapType = 0
    bearCap = 0
    print(f"发送 notifyMessage: tid={tid}, oid={oid}, did={did}, topic={topic}, act={act}, context={context}, coopMap(bytes)='{coopMap_bytes}', coopMapType={coopMapType}, bearCap={bearCap}")
    server.notifyMessage(tid, oid, did, topic, act, context, coopMap_bytes, coopMapType, bearCap) # Pass bytes
if __name__ == "__main__":
    main()