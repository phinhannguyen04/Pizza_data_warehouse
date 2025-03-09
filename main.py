import pandas as pd
from sqlServerConnection import SqlServerConnection



""" Tạo Staging Area và Data Warehouse
    - B1 : tạo một csdl trong sql server
    - B2 : lấy chuỗi kết noi tới sql server
    - B3 : đọc file exel
    - B4 : chuyển dữ liệu từ file excel vao dataframe
    - B5 : tạo các bảng vơi các cột tương ứng trong dataframe
    - B6 : thêm dữ liệu vào các bảng
    - B7 : tạo FactSale Table kết nối với các bảng để lấy giá trị liên quan
    - B8 : tính toán các giá trị trong fact table
    - B9 : lấy gia tri tu bang fact table vao file output
    - B10: xư ly du liệu 
"""


# connect to sql server
db = SqlServerConnection("ODBC Driver 17 for SQL Server", ".\\SQLEXPRESS", "Demo")
conn = db.connection()

# read xlsx file

"""
Cài đặt thư viện pandas và nhúng openpyxl
    pip install pandas 
    pip intstall openpyxl
"""

PATH = "Data.xlsx"

df = pd.read_excel(PATH, index_col=None)

print(df.head())

# data tables
"""------------------------------------------------------------------------------------------------------------------"""
print('pizza_categories')
category_columns = ('pizza_categories_id', 'pizza_category')

pizza_category = db.import_data_to_dataframe(RAW_DATAFRAME=df, tuple_dataframe_columns=category_columns, the_firts_columns_is_null=True)
# Lọc bo cac hang trung nhau dua tren cac gia tri trung nhau trong cot pizza_category

pizza_category = pizza_category.drop_duplicates(subset='pizza_category')
print(pizza_category)


"""------------------------------------------------------------------------------------------------------------------"""
# print('pizza')

pizza_column = ("pizza_id", "pizza_name", "pizza_size", "unit_price", "pizza_category", "pizza_ingredients")
pizzas = db.import_data_to_dataframe(RAW_DATAFRAME=df, tuple_dataframe_columns=pizza_column)

# print(pizzas.head())

pizzas = pizzas.merge(
    pizza_category,
    how     ='left',
    left_on ='pizza_category',
    right_on='pizza_category'
)

# drop tất cả cột pizza_category do lúc này đang merge 2 bang lai
pizzas.drop(columns=['pizza_category'], inplace=True)
# doi ten cot pizza_categories_id trong pizzas dataFrame sang pizza_category_id cho giong voi cot da ton tai trong csdl
pizzas.rename(columns={'pizza_categories_id': 'pizza_category_id'}, inplace=True)
# gán kết qua drop_duplicates()
pizzas = pizzas.drop_duplicates()

print(pizzas)

"""------------------------------------------------------------------------------------------------------------------"""
print('orders')
order_columns = ('order_id', 'order_date', 'order_time', 'total_price')
orders = db.import_data_to_dataframe(df, order_columns, set_zero=True, column_set_zero='total_price')

orders.drop_duplicates(subset='order_id', inplace=True)
print(orders)

"""------------------------------------------------------------------------------------------------------------------"""
print("order_details")

order_detail_columns = ('order_details_id', 'details', 'quantity', 'total_price', 'pizza_id', 'order_id')
order_details = db.import_data_to_dataframe(RAW_DATAFRAME=df, tuple_dataframe_columns=order_detail_columns, accept_null=True, column_accept_null='details')

print(order_details.head())

"""------------------------------------------------------------------------------------------------------------------"""
"""
    Chuyển dữ liệu vào csdl
"""

db.import_data_to_sql(pizza_category, "pizza_categories")
db.import_data_to_sql(pizzas, "pizzas")
db.import_data_to_sql(orders, "orders")
db.import_data_to_sql(order_details, "order_details")

# import csv
#
# demo_dataframe = db.export_data("FactSales")
# FILE_PATH = 'Output_Data.csv'
# list_column_names = ["order_id", "sale_date", "sale_time", "order_total_price", "order_details_id", "quantity", "total_price", "pizza_id", "pizza_category"]

# db.export_to_csv(FILE_PATH=FILE_PATH, list_column_names=list_column_names, data_frame=demo_dataframe)
