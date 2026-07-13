import mysql.connector
from mysql.connector import Error

class DatabaseConfig:
    # Thông tin kết nối - CHỈNH SỬA THEO MÔI TRƯỜNG CỦA BẠN
    HOST = 'localhost'
    PORT = 3306
    DATABASE = 'stock_db'
    USER = 'root'
    PASSWORD = '123456'  # Thay đổi password của bạn
    
    @staticmethod
    def get_connection():
        try:
            connection = mysql.connector.connect(
                host=DatabaseConfig.HOST,
                port=DatabaseConfig.PORT,
                database=DatabaseConfig.DATABASE,
                user=DatabaseConfig.USER,
                password=DatabaseConfig.PASSWORD,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=False  # Tắt autocommit để kiểm soát transaction
            )
            
            if connection.is_connected():
                db_info = connection.get_server_info()
                print(f"Kết nối thành công tới MySQL Server version {db_info}")
                print(f"Database: {DatabaseConfig.DATABASE}")
                return connection
                
        except Error as e:
            print(f"Lỗi kết nối MySQL: {e}")
            return None
    
    @staticmethod
    def close_connection(connection):
        if connection and connection.is_connected():
            connection.close()
            print("\n Đã đóng kết nối MySQL")
    
    @staticmethod
    def test_connection():
        connection = DatabaseConfig.get_connection()
        if connection:
            DatabaseConfig.close_connection(connection)
            return True
        return False