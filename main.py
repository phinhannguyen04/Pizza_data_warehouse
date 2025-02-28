import pandas as pd
from sqlServerConnection import SqlServerConnection

# connect to sql server
db = SqlServerConnection("ODBC Driver 17 for SQL Server", "DESKTOP-M0CSCJB\\SQLEXPRESS", "Pizza")
conn = db.connection()

# read xlsx file

"""
Cài đặt thư viện pandas và nhúng openpyxl
    pip install pandas openpyxl
"""

PATH = "Data.xlsx"

df = pd.read_excel(PATH, index_col=None)

print(df.head())

# data tables
"""
pizza_categories
(
    pizza_categories_id, 
    pizza_category
)
"""
print('pizza_categories')

# column
pizza_category = df['pizza_category'].unique()

# table: tạo dataFrame pizza_categories
pizza_categories = pd.DataFrame({
    'pizza_categories_id': range(1, len(pizza_category) + 1),
    "pizza_category": pizza_category
})

print(pizza_categories)


"""
pizzas
(
    pizza_id
    pizza_name
    pizza_size
    unit_price
    category_id
    pizza_ingredients
)
"""
print('pizzas')

# column
pizza_id = df['pizza_id']
pizza_name = df['pizza_name']
pizza_size = df['pizza_size']
unit_price = df['unit_price']
# cần gắn id thay vì tên
pizza_category = df['pizza_category']
pizza_ingredients = df['pizza_ingredients']

# table
pizzas = pd.DataFrame({
    'pizza_id' : pizza_id,
    'pizza_name': pizza_name,
    'pizza_size': pizza_size,
    'unit_price' : unit_price,
    'pizza_category' : pizza_category,
    'pizza_ingredients' : pizza_ingredients
})

print(pizzas.head())

"""
Tạo khóa ngoại cho bảng pizza từ tên của pizza_categories
"""
# merge pizzas and pizza_categories

pizzas = pizzas.merge(
    pizza_categories,
    how     ='left',
    left_on ='pizza_category',
    right_on='pizza_category'
)

# drop tất cả cột pizza_category do lúc này đang merge 2 bang lai
pizzas.drop(columns=['pizza_category'], inplace=True)

# doi ten cot pizza_categories_id trong pizzas dataFrame sang pizza_category_id cho giong voi cot da ton tai trong csdl
pizzas.rename(columns={'pizza_categories_id': 'pizza_category_id'}, inplace=True)

print(len(pizzas))

# table
# gán kết qua drop_duplicates()
pizzas = pizzas.drop_duplicates()

print(pizzas)


"""
orders
(
    order_id
    order_date
    order_time
    total_price
    
    order_detail_id
)
"""

print('orders')

order_id    = df['order_id']
order_date  = df['order_date']
order_time  = df['order_time']
total_price = 0
# order_details_id = df['order_details_id']

orders = pd.DataFrame({
    'order_id' : order_id,
    'order_date': order_date,
    'order_time': order_time,
    'total_price': total_price,
})

orders.drop_duplicates(subset='order_id', inplace=True)

print(orders)

"""
order_details
(
    order_details_id
    details
    quantity
    total_price
    pizza_id
)
"""

print("order_details")

order_details_id    = df['order_details_id']
details             = df['details'] = 'null'
quantity            = df['quantity']
total_price         = df['total_price']
pizza_id            = df['pizza_id']
order_id            = df['order_id']

order_details = pd.DataFrame({
    'order_details_id'  : order_details_id,
    'details'           : details,
    'quantity '         : quantity,
    'total_price'       : total_price,
    'pizza_id'          : pizza_id,
    'order_id'          : order_id
})

print(order_details)

"""
    Chuyển dữ liệu vào csdl
"""

# db.import_data(pizza_categories, "pizza_categories")
# db.import_data(pizzas, "pizzas")
# db.import_data(orders, "orders")
db.import_data(order_details, "order_details")
