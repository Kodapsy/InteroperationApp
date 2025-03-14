import threading

class CapabilityManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        """初始化实例"""
        if not hasattr(self, 'initialized'):
            self.capabilities = []
            self.initialized = True

    @staticmethod
    def getInstance():
        """获取单例实例"""
        if CapabilityManager._instance is None:
            with CapabilityManager._lock:
                if CapabilityManager._instance is None:
                    CapabilityManager._instance = CapabilityManager()
        return CapabilityManager._instance

    def putCapability(self, appid: int, capId: int, capVersion: int, capConfig: int):
        """添加能力"""
        instance = CapabilityManager.getInstance()

        # 检查能力是否已存在
        for capability in instance.capabilities:
            if (capability['appid'] == appid and
                capability['capId'] == capId and
                capability['capVersion'] == capVersion and
                capability['capConfig'] == capConfig):
                print(f"Capability ({appid}, {capId}, {capVersion}, {capConfig}) already exists, skipping insert.")
                return  # 如果已存在，不再插入

        instance.capabilities.append({
            'appid': appid,
            'capId': capId,
            'capVersion': capVersion,
            'capConfig': capConfig
        })
    
    def deleteCapability(self, appid: int = None, capId: int = None, capVersion: int = None, capConfig: int = None):
        """删除满足条件的能力"""
        instance = CapabilityManager.getInstance()
        with CapabilityManager._lock:
            new_capabilities = []
            for capability in instance.capabilities:
                if appid is not None and capability['appid'] != appid:
                    new_capabilities.append(capability)
                elif capId is not None and capability['capId'] != capId:
                    new_capabilities.append(capability)
                elif capVersion is not None and capability['capVersion'] != capVersion:
                    new_capabilities.append(capability)
                elif capConfig is not None and capability['capConfig'] != capConfig:
                    new_capabilities.append(capability)
            instance.capabilities = new_capabilities

    def getCapability(self):
        """返回当前所有的能力"""
        instance = CapabilityManager.getInstance()
        with CapabilityManager._lock:
            return instance.capabilities.copy()
