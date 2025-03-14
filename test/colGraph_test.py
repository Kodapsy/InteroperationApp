import sys
import os
import threading
import time

# 将项目根目录添加到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from module.CollaborationGraphManager import CollaborationGraphManager



def test_collaboration_graph_manager():
    # 获取单例实例
    manager = CollaborationGraphManager.getInstance()

    # 插入映射关系
    manager.updateMapping(123456, [(1, 2, 3), (1, 2, 4), (2, 3, 5)])
    
    # 查询特定映射关系的设备
    devices = manager.getDevices(1, 2, 3)
    print(f"Devices for (1, 2, 3): {devices}")  # 输出: [123456]
    
    devices = manager.getDevices(1, 2, 4)
    print(f"Devices for (1, 2, 4): {devices}")  # 输出: [123456]
    
    devices = manager.getDevices(2, 3, 5)
    print(f"Devices for (2, 3, 5): {devices}")  # 输出: [123456]
    
    # 插入新的映射关系
    manager.updateMapping(22222, [(1, 2, 3), (1, 2, 4)])

    # 查询更新后的设备列表
    devices = manager.getDevices(1, 2, 3)
    print(f"Devices for (1, 2, 3) after adding 22222: {devices}")  # 输出: [123456, 22222]
    
    devices = manager.getDevices(1, 2, 4)
    print(f"Devices for (1, 2, 4) after adding 22222: {devices}")  # 输出: [123456, 22222]

    # 重复插入已经存在的 deviceId，原映射不做改变
    manager.updateMapping(123456, [(1, 2, 3)])

    # 查询设备列表，确保没有重复插入
    devices = manager.getDevices(1, 2, 3)
    print(f"Devices for (1, 2, 3) after attempting to re-add 123456: {devices}")  # 输出: [123456, 22222]

    # 插入不同的映射，删除无效的 deviceId 映射
    manager.updateMapping(33333, [(2, 3, 5)])

    # 查询设备列表，确保删除无效的映射
    devices = manager.getDevices(2, 3, 5)
    print(f"Devices for (2, 3, 5) after adding 33333: {devices}")  # 输出: [123456, 33333]

    # 确认之前的设备 123456 被从无效的映射中删除
    devices = manager.getDevices(1, 2, 4)
    print(f"Devices for (1, 2, 4) after 123456 removal: {devices}")  # 输出: [22222]

if __name__ == "__main__":
    test_collaboration_graph_manager()
