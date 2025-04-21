import threading
from .logger import logger_decorator, global_logger

# 启用模块日志
global_logger.enable_module("CapabilityManager")

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

    @logger_decorator("CapabilityManager", level="INFO")
    def putCapability(self, appid: int, capId: int, capVersion: int, capConfig: int) -> bool:
        """添加能力，默认broadcast为True"""
        logger_test = global_logger.get_logger("CapabilityManager")
        logger_test.info(f"Adding capability ({appid}, {capId}, {capVersion}, {capConfig})")
        instance = CapabilityManager.getInstance()

        # 检查能力是否已存在
        for capability in instance.capabilities:
            if (capability['appid'] == appid and
                capability['capId'] == capId and
                capability['capVersion'] == capVersion and
                capability['capConfig'] == capConfig):
                print(f"Capability ({appid}, {capId}, {capVersion}, {capConfig}) already exists, skipping insert.")
                return False

        instance.capabilities.append({
            'appid': appid,
            'capId': capId,
            'capVersion': capVersion,
            'capConfig': capConfig,
            'broadcast': True
        })
        return True
    
    @logger_decorator("CapabilityManager", level="INFO")
    def deleteCapability(self, appid: int = None, capId: int = None, capVersion: int = None, capConfig: int = None) -> bool:
        """删除满足条件的能力，正常返回 True，异常返回 False"""
        try:
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
            return True
        except Exception as e:
            print(f"An error occurred in deleteCapability: {e}")
            return False
        
    
    def updateBroadcast(self, appid: int = None, capId: int = None, capVersion: int = None, capConfig: int = None, broadcast: bool = None) -> bool:
        """
        更新满足条件的能力的 broadcast 值，参数为None则不参与匹配，
        只有当 broadcast 参数不为 None 时才进行更新。
        """
        if broadcast is None:
            print("No broadcast value provided, nothing to update.")
            return False
        
        try:
            instance = CapabilityManager.getInstance()
            with CapabilityManager._lock:
                for capability in instance.capabilities:
                    # 对于传入的不为None的条件，都必须匹配
                    if (appid is not None and capability['appid'] != appid):
                        continue
                    if (capId is not None and capability['capId'] != capId):
                        continue
                    if (capVersion is not None and capability['capVersion'] != capVersion):
                        continue
                    if (capConfig is not None and capability['capConfig'] != capConfig):
                        continue
                    # 更新匹配的能力的 broadcast 值
                    capability['broadcast'] = broadcast
                    # print(f"Updated capability ({capability['appid']}, {capability['capId']}, {capability['capVersion']}, {capability['capConfig']}) broadcast to {broadcast}.")
            
            return True
        except Exception as e:
            print(f"An error occurred updateBroadcast: {e}")
            return False

    def getCapability(self):
        """返回当前所有broadcast为True的能力"""
        instance = CapabilityManager.getInstance()
        with CapabilityManager._lock:
            # 只返回broadcast为True的能力
            return [cap for cap in instance.capabilities if cap.get('broadcast', True)]
