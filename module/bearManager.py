import threading

class bearManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        """初始化实例，存储承载关系的字典"""
        if not hasattr(self, 'initialized'):
            # 使用字典存储，键为 (通信目的端, 承载类型) 的元组，
            # 值为 (承载地址, portNum) 的元组，其中承载地址为 string，portNum 为 int
            self.bear_map = {}
            self.initialized = True

    @staticmethod
    def getInstance():
        """获取单例实例"""
        if bearManager._instance is None:
            with bearManager._lock:
                if bearManager._instance is None:
                    bearManager._instance = bearManager()
        return bearManager._instance

    def update(self, comm_dest: str, bear_type: str, address: str, portNum: int) -> bool:
        """
        更新承载关系：
        - 如果 (通信目的端, 承载类型) 的记录已存在，则覆盖其承载地址和 portNum；
        - 如果不存在，则新增记录。
        """
        instance = bearManager.getInstance()
        with bearManager._lock:
            instance.bear_map[(comm_dest, bear_type)] = (address, portNum)
        return True

    def query(self, comm_dest: str = None, bear_type: str = None):
        """
        查询承载关系：
        - 当两个参数均不为空时，进行等值查询；
        - 允许其中一个参数为空，此时返回匹配非空参数的所有记录；
        - 当两个参数都为空时，抛出异常。
        
        返回值为一个列表，每个元素为字典，格式为：
            {
              'comm_dest': <通信目的端>,
              'bear_type': <承载类型>,
              'address': <承载地址>,
              'portNum': <端口号>
            }
        """
        if (comm_dest is None or comm_dest == "") and (bear_type is None or bear_type == ""):
            raise ValueError("通信目的端和承载类型不能同时为空")

        instance = bearManager.getInstance()
        results = []
        with bearManager._lock:
            for (dest, btype), (addr, port) in instance.bear_map.items():
                # 当指定了通信目的端时必须匹配
                if comm_dest and dest != comm_dest:
                    continue
                # 当指定了承载类型时必须匹配
                if bear_type and btype != bear_type:
                    continue
                results.append({
                    'comm_dest': dest,
                    'bear_type': btype,
                    'address': addr,
                    'portNum': port
                })
        return results