import mysql.connector
from mysql.connector import Error
from connect_to_mysql import connect_to_mysql

def create_table():
    """ 创建数据库表 """
    try:

        
        # 连接到 MySQL
        conn = connect_to_mysql("../data/mysql.json")
        cursor = conn.cursor()

        # 创建数据库
        cursor.execute("CREATE DATABASE IF NOT EXISTS capability_db")
        cursor.execute("USE capability_db")

        # 创建能力表
        create_table_query = """
        CREATE TABLE IF NOT EXISTS capabilities (
            CapID INT NOT NULL PRIMARY KEY,  -- 能力标识 (CapID)
            CapVer INT NOT NULL,             -- 能力版本 (CapVer)
            CapConfig INT NOT NULL          -- 能力配置 (CapConfig)
        );
        """
        cursor.execute(create_table_query)
        print("表 'capabilities' 创建成功！")

    except Error as e:
        print(f"创建表时出错: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 执行创建表的函数
if __name__ == "__main__":
    create_table()
