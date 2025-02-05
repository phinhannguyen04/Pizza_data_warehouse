import pandas as pd
from sqlServerConnection import SqlServerConnection

# connect to sql server
db = SqlServerConnection("ODBC Driver 17 for SQL Server", "DESKTOP-M0CSCJB\\SQLEXPRESS", "PizzaReported")
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

# merge pizzas and pizza_categories

pizzas = pizzas.merge(
    pizza_categories,
    how     ='left',
    left_on ='pizza_category',
    right_on='pizza_category'
)

# drop cột category do lúc này đang merge 2 bang lai
pizzas.drop(columns=['pizza_category'], inplace=True)

# doi ten cot pizza_category trong pizzas dataFrame sang category_id
pizzas.rename(columns={'pizza_categories_id': 'category_id'}, inplace=True)

print(len(pizzas))

# table
pizzas.drop_duplicates()

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
total_price = df['total_price'] = 0
# order_details_id = df['order_details_id']

orders = pd.DataFrame({
    'order_id' : order_id,
    'order_date': order_date,
    'order_time': order_time,
    'total_price': total_price,
    # 'order_details_id': order_details_id
})

orders.drop_duplicates(subset='order_id', inplace=True)

print(orders)