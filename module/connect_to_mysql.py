import json
import mysql.connector

# 读取 mysql.json 配置文件
def load_mysql_config(mysql_config_path):
    with open(mysql_config_path, 'r') as f:
        config = json.load(f)
    return config

# 使用 mysql 配置连接到数据库
def connect_to_mysql(mysql_config_path):
    try:
        # 加载配置
        config = load_mysql_config(mysql_config_path)

        # 使用读取的配置连接 MySQL 数据库
        conn = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        
        print(f"成功连接到MYSQL")
        
        # 创建游标
        cursor = conn.cursor()
        
        return conn

    except mysql.connector.Error as e:
        print(f"数据库连接错误: {e}")
    except FileNotFoundError:
        print("未找到 mysql.json 文件。请确保该文件存在。")
    except json.JSONDecodeError:
        print("读取 mysql.json 文件时发生错误，可能文件格式不正确。")

# 执行连接数据库操作
if __name__ == "__main__":
    connect_to_mysql("../data/mysql.json")
