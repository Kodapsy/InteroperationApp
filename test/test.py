import time
import json
import sys
sys.path.append("/home/nvidia/mydisk/czl/InteroperationApp")
#sys.path.append("/home/czl/InteroperationApp")
sys.path.append("/home/czl/InteroperationApp")
from module.zmq_server import ICPServer, ICPClient

def main():
    print("🚀 ICPServer 测试程序启动")
    app_id = input("请输入 app_id（默认 0）: ") or 0
    server = ICPServer(app_id=app_id)

    while True:
        print("\n请选择操作类型:")
        print("1. 注册/注销能力 (AppMessage)")
        print("2. 广播发布 (brocastPub)")
        print("3. 广播订购 (brocastSub)")
        print("4. 广播订购通知 (brocastSubnty)")
        print("5. 精准订阅 (subMessage)")
        print("6. 精准通知 (notifyMessage)")
        print("7. 流发送请求 (streamSendreq)")
        print("8. 流发送 (streamSend)")
        print("9. 流发送结束 (streamSendend)")
        print("10. 文件发送 (sendFile)")
        print("0. 退出")

        try:
            choice = int(input("请输入操作编号: ").strip())
        except ValueError:
            print("❌ 无效输入，请输入数字")
            continue

        if choice == 0:
            print("✅ 程序退出")
            break

        try:
            if choice == 1:
                CapID = int(input("CapID: "))
                CapVersion = int(input("CapVersion: "))
                CapConfig = int(input("CapConfig: "))
                act = int(input("操作（0注册，1注销，2广播打开，3广播关闭）: "))
                tid = int(input("tid (事务ID): "))
                server.AppMessage(CapID, CapVersion, CapConfig, act, tid)

            elif choice == 2:
                tid = int(input("tid: "))
                oid = input("oid（源端ID）: ")
                topic = int(input("topic: "))
                coopMap_input = input("coopMap 数据（字符串）: ")
                coopMap = coopMap_input.encode()  # 先转 bytes
                coopMap = coopMap.hex()       # 再转十六进制字符串
                coopMapType = int(input("coopMapType: "))
                server.brocastPub(tid, oid, topic, coopMap, coopMapType)

            elif choice == 3:
                tid = int(input("tid: "))
                oid = input("oid: ")
                topic = int(input("topic: "))
                context = input("context（二进制字符串 128 位）: ")
                coopMap_input = input("coopMap: ")
                coopMap = coopMap_input.encode()  # 先转 bytes
                coopMap = coopMap.hex()
                coopMapType = int(input("coopMapType: "))
                bearCap = int(input("bearCap (1 代表需要承载能力): "))
                server.brocastSub(tid, oid, topic, context, coopMap, coopMapType, bearCap)

            elif choice == 4:
                tid = 1
                oid = "A12345"
                did = "津A12345"
                topic = 12345
                context = "ffffffffffffffffffffffffffffffff"
                coopMap_input = "11111111111111111111"
                coopMap = coopMap_input.encode()  # 先转 bytes
                coopMap = coopMap.hex()
                coopMapType = 0
                bearCap = 0
                server.brocastSubnty(tid, oid, did, topic, context, coopMap, coopMapType, bearCap)

            elif choice == 5:
                tid = int(input("tid: "))
                oid = input("oid: ")
                did = input("did（用逗号分隔多个）: ").split(',')
                topic = int(input("topic: "))
                act = int(input("act 操作: "))
                context = input("context（二进制字符串 128 位）: ")
                coopMap_input = input("coopMap: ")
                coopMap = coopMap_input.encode()  # 先转 bytes
                coopMap = coopMap.hex()
                coopMapType = int(input("coopMapType: "))
                bearInfo = int(input("bearInfo: "))
                server.subMessage(tid, oid, did, topic, act, context, coopMap, coopMapType, bearInfo)

            elif choice == 6:
                tid = 1
                oid = "A12345"
                did = "津A12345"
                topic = "12345"
                act = "1"
                context = "ffffffffffffffffffffffffffffffff"
                coopMap_input = "111111111111111111111111111111111111"
                coopMap = coopMap_input.encode()  # 先转 bytes
                coopMap = coopMap.hex()
                coopMapType = 0
                bearCap = 0
                server.notifyMessage(tid, oid, did, topic, act, context, coopMap, coopMapType, bearCap)

            elif choice == 7:
                did = input("did: ")
                context = input("context: ")
                rl = int(input("RL (默认 1): ") or "1")
                pt = int(input("Payload 类型: "))
                server.streamSendreq(did, context, rl, pt)

            elif choice == 8:
                sid = input("sid: ")
                data = input("流数据内容: ")
                server.streamSend(sid, data)

            elif choice == 9:
                did = input("did: ")
                context = input("context: ")
                sid = input("sid: ")
                server.streamSendend(did, context, sid)

            elif choice == 10:
                did = input("did: ")
                context = input("context: ")
                rl = int(input("RL: "))
                pt = int(input("Payload 类型: "))
                file_path = input("文件路径: ")
                server.sendFile(did, context, rl, pt, file_path)

            else:
                print("❌ 不支持的操作编号")

        except Exception as e:
            print(f"❗ 操作失败：{e}")

        print("✅ 操作已发送，等待处理...\n")

if __name__ == "__main__":
    main()
