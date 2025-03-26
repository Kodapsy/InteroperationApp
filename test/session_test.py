import sys
sys.path.append("/home/nvidia/mydisk/czl/InteroperationApp/module")
from sessionManager import SessionManager
def interactive_test():
    sm = SessionManager.getInstance()

    print("欢迎进入 SessionManager 测试模式")
    print("输入 'exit' 退出测试")
    
    while True:
        try:
            mid_input = input("请输入 mid: ")
            if mid_input.lower() == "exit":
                break
            mid = int(mid_input)

            context_input = input("请输入 context: ")
            if context_input.lower() == "exit":
                break
            context = str(context_input)

            act_input = input("请输入 act (可选, 按回车跳过): ")
            act = int(act_input) if act_input.strip() else None

            result = sm.update_state(mid=mid, context=context, act=act)
            print(f"状态更新结果: {result}")
            print(f"当前会话状态: {sm.sessions}")

        except ValueError:
            print("输入错误，请输入有效的整数")

if __name__ == "__main__":
    interactive_test()