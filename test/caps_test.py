import sys
import os

# 将项目根目录添加到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from module.capsManager import capsManager

# 测试示例
if __name__ == "__main__":
    # 配置 MySQL 数据库连接参数
   
    # 创建能力管理对象
    manager = capsManager("../data/mysql.json")

    # 添加能力
    manager.add_capability(1, 2, 123)
    manager.add_capability(2, 1, 321)

    # 查询所有能力
    print("\n所有能力:")
    print(manager.get_all_capabilities())

    # 删除能力
    #manager.remove_capability(1)
    # manager.remove_capability(2)

    # 查询所有能力
    print("\n删除能力后的所有能力:")
    print(manager.get_all_capabilities())

    # 关闭数据库连接
    manager.close()