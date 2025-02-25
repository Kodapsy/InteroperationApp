import mysql.connector
from mysql.connector import Error
from module.connect_to_mysql import connect_to_mysql

class capsManager:
    def __init__(self, mysql_config_path):
        """ 初始化数据库连接 """
        self.mysql_config_path = mysql_config_path
        self.conn = None
        self.cursor = None
        self.connect(mysql_config_path)

    def connect(self,mysql_config_path):
        """ 连接到 MySQL 数据库 """
        self.conn = connect_to_mysql(mysql_config_path)
        self.cursor = self.conn.cursor()

    def close(self):
        """ 关闭数据库连接 """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def add_capability(self, cap_id, cap_ver, cap_config):
        """
        增加一个新的能力到数据库
        :param cap_id: 能力标识 (CapID)
        :param cap_ver: 能力版本 (CapVer)
        :param cap_config: 能力配置 (CapConfig)
        """
        try:
            # 插入新的能力
            query = "INSERT INTO capabilities (CapID, CapVer, CapConfig) VALUES (%s, %s, %s)"
            values = (cap_id, cap_ver, cap_config)
            self.cursor.execute(query, values)
            self.conn.commit()
            print(f"成功添加能力: CapID={cap_id}, CapVer={cap_ver}, CapConfig={cap_config}")
        except Error as e:
            print(f"添加能力失败: {e}")

    def remove_capability(self, cap_id):
        """
        删除指定能力标识的能力
        :param cap_id: 能力标识 (CapID)
        """
        try:
            query = "DELETE FROM capabilities WHERE CapID = %s"
            self.cursor.execute(query, (cap_id,))
            self.conn.commit()
            print(f"成功删除能力: CapID={cap_id}")
        except Error as e:
            print(f"删除能力失败: {e}")

    def get_all_capabilities(self):
        """ 查询所有能力并返回 """
        try:
            query = "SELECT * FROM capabilities"
            self.cursor.execute(query)
            capabilities = self.cursor.fetchall()
            result = []
            for cap in capabilities:
                result.append({
                    "CapID": cap[0],
                    "CapVer": cap[1],
                    "CapConfig": cap[2]
                })
            return result
        except Error as e:
            print(f"查询能力失败: {e}")
            return []

    def __repr__(self):
        """ 返回所有能力的字符串表示 """
        return str(self.get_all_capabilities())


