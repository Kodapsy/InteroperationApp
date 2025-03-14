import threading

class CollaborationGraphManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        """初始化实例"""
        if not hasattr(self, 'initialized'):
            self.mapping = {}  # 字典，用于存储 {capId, capVersion, capConfig} -> [deviceId]
            self.initialized = True

    @staticmethod
    def getInstance():
        """获取单例实例"""
        if CollaborationGraphManager._instance is None:
            with CollaborationGraphManager._lock:
                if CollaborationGraphManager._instance is None:
                    CollaborationGraphManager._instance = CollaborationGraphManager()
        return CollaborationGraphManager._instance

    def updateMapping(self, deviceId: int, capList: list):
        """根据输入的 deviceId 和 capList 更新映射关系"""
        with CollaborationGraphManager._lock:
            for cap in capList:
                capId, capVersion, capConfig = cap
                key = (capId, capVersion, capConfig)  # 将 {capId, capVersion, capConfig} 作为键
                
                if key not in self.mapping:
                    # 如果该键不存在，则直接添加
                    self.mapping[key] = [deviceId]
                else:
                    # 如果该键已经存在，检查是否需要更新
                    current_devices = self.mapping[key]
                    
                    if deviceId not in current_devices:
                        # 如果 deviceId 不在当前列表中，更新并添加 deviceId
                        self.mapping[key].append(deviceId)

                    # 清理没有匹配的 deviceId
                    for other_key, devices in list(self.mapping.items()):
                        if other_key != key and deviceId in devices:
                            self.mapping[other_key].remove(deviceId)
                            if not self.mapping[other_key]:
                                del self.mapping[other_key]

    def getDevices(self, capId: int, capVersion: int, capConfig: int):
        """查询并返回对应 {capId, capVersion, capConfig} 的所有 deviceId"""
        key = (capId, capVersion, capConfig)
        with CollaborationGraphManager._lock:
            # 如果该键存在，返回对应的 deviceId 列表，否则返回空列表
            return self.mapping.get(key, [])
