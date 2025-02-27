import sys
import os
import threading

# 将项目根目录添加到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from module.abilityManager import AbilityManager

def update_in_thread(role, abilities):
    # 获取单例实例并更新角色的能力
    manager = AbilityManager()
    manager.update_abilities(role, abilities)


def query_in_main():
    # 获取单例实例并查询所有角色的能力
    manager = AbilityManager()
    abilities = manager.query_abilities()
    for role, abilities_set in abilities.items():
        print(f"角色 {role} 的能力: {', '.join(map(str, abilities_set))}")

# 测试示例
if __name__ == "__main__":
   # 获取单例实例
    manager = AbilityManager()

    # 创建两个线程，分别更新角色的能力
    thread1 = threading.Thread(target=update_in_thread, args=("A", [1, 2, 3, 4]))
    thread2 = threading.Thread(target=query_in_main)

    # 启动线程
    thread1.start()
    thread2.start()


    # 等待线程结束
    thread1.join()
    thread2.join()

    # # 查询并输出所有角色的能力
    # query_in_main()