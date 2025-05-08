import time
import json
import sys

# Ensure the paths are correct for your environment
sys.path.append("/home/nvidia/mydisk/czl/InteroperationApp")
# sys.path.append("/home/czl/InteroperationApp") # Can be removed if covered

from module.zmq_server import ICPServer, ICPClient # Assuming zmq_server.py contains the ICPServer expecting bytes for coopMap
import config # Import the original config module

def main():
    print("🚀 ICPServer 测试程序启动")

    app_id_input = input("请输入 app_id（默认 0）: ")
    app_id = int(app_id_input) if app_id_input else 0

    try:
        print(f"使用配置: selfip={config.selfip}, send_sub_port={config.send_sub_port}")
        server = ICPServer(app_id=app_id)
    except Exception as e:
        print(f"❌ 初始化服务器失败: {e}")
        return

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
            choice_input = input("请输入操作编号: ").strip()
            if not choice_input:
                print("❌ 未输入操作编号，请重试。")
                continue
            choice = int(choice_input)
        except ValueError:
            print("❌ 无效输入，请输入数字。")
            continue

        if choice == 0:
            print("✅ 程序退出")
            break

        try:
            if choice == 1:
                CapID = int(input("CapID: "))
                CapVersion = int(input("CapVersion: "))
                CapConfig = int(input("CapConfig: "))
                act_prompt = f"操作 ({config.appActLogin}-注册, {config.appActLogout}-注销, {config.appActopen}-广播打开, {config.appActclose}-广播关闭): "
                act = int(input(act_prompt))
                tid = int(input("tid (事务ID, 默认为0): ") or "0")
                server.AppMessage(CapID, CapVersion, CapConfig, act, tid)

            elif choice == 2: # brocastPub
                tid = int(input("tid (默认为0): ") or "0")
                oid = input("oid（源端ID）: ")
                topic = int(input("topic: "))
                coopMap_input = input("coopMap 数据 (普通字符串，将进行UTF-8编码为bytes): ")
                coopMap_bytes = coopMap_input.encode('utf-8') # Encode to bytes
                coopMapType = int(input("coopMapType: "))
                server.brocastPub(tid, oid, topic, coopMap_bytes, coopMapType) # Pass bytes

            elif choice == 3: # brocastSub
                tid = int(input("tid (默认为0): ") or "0")
                oid = input("oid: ")
                topic = int(input("topic: "))
                context = input("context (例如: ffffffffffffffffffffffffffffffff): ")
                coopMap_input = input("coopMap 数据 (普通字符串，将进行UTF-8编码为bytes): ")
                coopMap_bytes = coopMap_input.encode('utf-8') # Encode to bytes
                coopMapType = int(input("coopMapType: "))
                bearCap = int(input("bearCap (1 代表需要承载能力): "))
                server.brocastSub(tid, oid, topic, context, coopMap_bytes, coopMapType, bearCap) # Pass bytes

            elif choice == 4: # brocastSubnty with example values
                tid = 1
                oid = "A12345"
                did = "津A12345"
                topic = 12345
                context = "ffffffffffffffffffffffffffffffff"
                coopMap_input = "示例协作图数据"
                coopMap_bytes = coopMap_input.encode('utf-8') # Encode to bytes
                coopMapType = 0
                bearCap = 0
                print(f"发送 brocastSubnty: tid={tid}, oid={oid}, did={did}, topic={topic}, context={context}, coopMap(bytes)='{coopMap_bytes}', coopMapType={coopMapType}, bearCap={bearCap}")
                server.brocastSubnty(tid, oid, did, topic, context, coopMap_bytes, coopMapType, bearCap) # Pass bytes

            elif choice == 5: # subMessage
                tid = int(input("tid (默认为0): ") or "0")
                oid = input("oid: ")
                did_input = input("did（目标ID列表，用逗号分隔）: ")
                did = [d.strip() for d in did_input.split(',')]
                topic = int(input("topic: "))
                act = int(input("act 操作 (例如: 0-订阅, 1-取消订阅): "))
                context = input("context (例如: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee): ")
                coopMap_input = input("coopMap 数据 (普通字符串，将进行UTF-8编码为bytes): ")
                coopMap_bytes = coopMap_input.encode('utf-8') # Encode to bytes
                coopMapType = int(input("coopMapType: "))
                bearInfo = int(input("bearInfo (1 代表需要承载地址信息): "))
                server.subMessage(tid, oid, did, topic, act, context, coopMap_bytes, coopMapType, bearInfo) # Pass bytes

            elif choice == 6: # notifyMessage with example values
                tid = 1
                oid = "A12345"
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

            elif choice == 7:
                did = input("did (目的端ID): ")
                context = input("context: ")
                rl_input = input("RL (流数据质量保证, 默认 1): ")
                rl = int(rl_input) if rl_input else 1
                pt = int(input("pt (数据类型): "))
                server.streamSendreq(did, context, rl, pt)

            elif choice == 8:
                sid = input("sid (流标识): ")
                data = input("流数据内容: ")
                server.streamSend(sid, data)

            elif choice == 9:
                did = input("did (目的端ID): ")
                context = input("context: ")
                sid = input("sid (流标识): ")
                server.streamSendend(did, context, sid)

            elif choice == 10:
                did_input = input("did (目的端ID): ")
                try:
                    did = int(did_input)
                except ValueError:
                    print("❌ 'did' 必须是一个整数。")
                    continue
                context = input("context: ")
                rl_input = input("RL (流数据质量保证, 例如 1): ")
                rl = int(rl_input)
                pt = int(input("pt (数据类型): "))
                file_path = input("文件路径 (例如 /path/to/your/file.dat): ")
                server.sendFile(did, context, rl, pt, file_path)

            else:
                print("❌ 不支持的操作编号。")

        except ValueError as ve:
            print(f"❗ 输入错误: {ve}. 请确保输入了正确类型的数据。")
        except Exception as e:
            print(f"❗ 操作失败: {e}")

if __name__ == "__main__":
    main()