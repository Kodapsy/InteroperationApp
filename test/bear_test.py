import threading
import time
import sys
import os
# 将项目根目录添加到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from module.bearManager import bearManager

if __name__ == "__main__":
    bm = bearManager.getInstance()
    # 示例1：更新记录——例如自车A与B之间的IPv4通信，其中B的地址为 10.20.224.210，端口为 8080
    bm.update("B", "ipv4", "10.20.224.210", 8080)
    # 示例2：更新记录——例如自车A与C之间的IPv6通信，其中C的地址为 fe80::1，端口为 9090
    bm.update("C", "ipv6", "fe80::1", 9090)
    bm.update("C", "DSMP", "", 90)
    
    # 查询：指定通信目的端为B，承载类型为ipv4
    result1 = bm.query("B", "ipv4")
    print("查询结果1：", result1)
    
    # 查询：仅指定通信目的端（承载类型为空），查询所有通信目的端为C的记录
    result2 = bm.query("C", "")
    print("查询结果2：", result2)
    
    # 查询：仅指定承载类型（通信目的端为空），查询所有承载类型为ipv6的记录
    result3 = bm.query("", "ipv6")
    print("查询结果3：", result3)
    
    # 查询：当两个参数都为空时，将抛出异常
    try:
        bm.query("", "")
    except Exception as e:
        print("查询异常：", e)