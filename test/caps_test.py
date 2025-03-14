import threading
import time
import sys
import os
# 将项目根目录添加到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from module.CapabilityManager import CapabilityManager

def test_singleton_with_lock_and_deduplication():
    # 获取单例实例
    manager = CapabilityManager.getInstance()

    # 定义两个线程函数
    def worker1():
        for i in range(100):
            manager.putCapability(i, i*100, 1, 1)  # 插入能力
            time.sleep(0.01)

    def worker2():
        for i in range(100):
            manager.putCapability(i, i*102, 1, 2)  # 插入能力
            time.sleep(0.01)

    # 启动两个线程
    thread1 = threading.Thread(target=worker1)
    thread2 = threading.Thread(target=worker2)

    thread1.start()
    thread2.start()

    # 等待线程完成
    thread1.join()
    thread2.join()

    # 查看能力管理器中的能力列表
    capabilities = manager.getCapability()
    print("Total capabilities after both threads:", len(capabilities))
    print("Capabilities:", capabilities)

# 执行测试
test_singleton_with_lock_and_deduplication()
