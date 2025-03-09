"""
Connects to a SQL database using pyodbc
"""
import pyodbc
import csv
import pandas as pd
from pyodbc import connect


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

    # Convert import data to dataframe
    def import_data_to_dataframe(self, RAW_DATAFRAME, tuple_dataframe_columns, the_firts_columns_is_null=False,
                                 accept_null=False, column_accept_null=None, set_zero=False, column_set_zero=None):
        if not RAW_DATAFRAME.empty and tuple_dataframe_columns:
            columns_dataframe = []
            first_col = tuple_dataframe_columns[0]
            # Xử lý cột đầu tiên
            if the_firts_columns_is_null and first_col not in RAW_DATAFRAME.columns:
                columns_dataframe.append(pd.Series(range(1, len(RAW_DATAFRAME) + 1), name=first_col))
            elif first_col in RAW_DATAFRAME.columns and not RAW_DATAFRAME[first_col].isnull().all():
                columns_dataframe.append(RAW_DATAFRAME[first_col])
            else:
                print(f"Warning: Column '{first_col}' not found or contains only NaN values.")
                return None
            # Thêm các cột còn lại
            for col in tuple_dataframe_columns[1:]:
                if col in RAW_DATAFRAME.columns:
                    if set_zero and col == column_set_zero:
                        columns_dataframe.append(pd.Series([0] * len(RAW_DATAFRAME), name=col))  # Set cột bằng 0
                    elif accept_null and col == column_accept_null:
                        columns_dataframe.append(RAW_DATAFRAME[col])  # Chấp nhận Null
                    elif not RAW_DATAFRAME[col].isnull().all():
                        columns_dataframe.append(RAW_DATAFRAME[col])
                    else:
                        print(f"Warning: Column '{col}' contains only NaN values.")
                else:
                    if accept_null and col == column_accept_null:
                        columns_dataframe.append(
                            pd.Series([None] * len(RAW_DATAFRAME), name=col))  # Cột không tồn tại, set Null
                    else:
                        print(f"Warning: Column '{col}' not found.")
            # Tạo DataFrame
            if columns_dataframe:
                return pd.concat(columns_dataframe, axis=1)
            else:
                print("Error: No valid columns to create DataFrame.")
                return None
        else:
            print("DataFrame is empty or tuple_dataframe_columns is empty.")
            return None


    # Import data to sql
    def import_data_to_sql(self, df, table_name, column_names=None):
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

    # Lấy dữ liệu từ CSDL
    def export_data (self, table_name):
        conn = self.connection()
        cursor = conn.cursor()

        # Viet lenh lay lay ra cac gia tri trong bang
        sql = f"select * from {table_name}"
        cursor.execute(sql)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return rows

    # Nhap du lieu vào file csv
    def export_to_csv (self, FILE_PATH, list_column_names, data_frame):
        if data_frame:
            with open(f'{FILE_PATH}', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(list_column_names)
                for row in data_frame:
                    list_element = []
                    for element in row:
                        list_element.append(element)
                    writer.writerow(list_element)


# Test
# Giả sử pizza_categories là DataFrame của bạn
# Tạo một đối tượng SqlServerConnection
# db = SqlServerConnection(driver="...", server="...", database="...")
# db.import_data(pizza_categories, "pizza_categories")