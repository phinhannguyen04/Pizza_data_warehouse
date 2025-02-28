"""
Connects to a SQL database using pyodbc
"""
import pyodbc
import pandas as pd

class SqlServerConnection:
    DRIVER = ""
    SERVER = ""
    DATABASE = ""

    def __init__(self, driver: str, server: str, database: str):
        self.DRIVER = driver
        self.SERVER = server
        self.DATABASE = database

    # Kết noi toi sql server
    def connection(self):
        connection = pyodbc.connect(
            f"Driver={self.DRIVER};"
            f"Server={self.SERVER};"
            f"Database={self.DATABASE};"
            "Trusted_Connection=yes;"
        )
        print('Connection establish')
        return connection

    # Import du lieu
    def import_data(self, df, table_name, column_names=None):
        conn = self.connection()
        cursor = conn.cursor()

        if column_names is None:
            column_names = list(df.columns)  # Sử dụng tất cả các cột trong DataFrame

        # Tạo chuỗi tên cột cho lệnh INSERT
        columns_str = ", ".join([f"[{col}]" for col in column_names])

        # Tạo chuỗi placeholder cho lệnh INSERT
        placeholders = ", ".join(["?" for _ in column_names])

        # Tạo lệnh INSERT động
        insert_sql = f"INSERT INTO [{table_name}] ({columns_str}) VALUES ({placeholders})"

        # Lặp qua từng hàng của DataFrame và thực hiện lệnh INSERT
        for index, row in df.iterrows():
            values = tuple(row[col] for col in column_names)  # Lấy giá trị theo thứ tự cột
            cursor.execute(insert_sql, values)

        conn.commit()
        cursor.close()
        conn.close()

# Test
# Giả sử pizza_categories là DataFrame của bạn
# Tạo một đối tượng SqlServerConnection
# db = SqlServerConnection(driver="...", server="...", database="...")
# db.import_data(pizza_categories, "pizza_categories")