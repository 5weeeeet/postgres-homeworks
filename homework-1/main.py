import psycopg2
from psycopg2 import sql
import csv

# Параметры подключения к базе данных PostgreSQL
db_host = '192.168.179.128'
db_port = '13791'
db_name = 'north'
db_user = 'postgres'
db_password = '1'

# Путь к CSV файлу
csv_file = 'путь_к_файлу.csv'
employees_data = 'north_data/employees_data.csv'
customers_data = 'north_data/customers_data.csv'
orders_data = 'north_data/orders_data.csv'

# Имя таблицы, в которую необходимо загрузить данные
table_name = 'имя_таблицы'
employees = 'employees'
customers = 'customers'
orders = 'orders'

# Установка соединения с базой данных
conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_password)
cursor = conn.cursor()

# Очистка таблицы перед загрузкой данных
clear_table_query = sql.SQL("DELETE FROM {};").format(sql.Identifier(employees))
cursor.execute(clear_table_query)

with open(employees_data, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Пропускаем заголовки
    for row in reader:
        values = row
        placeholders = ', '.join(['%s' for _ in range(len(values))])
        insert_query = sql.SQL(f"INSERT INTO {employees} (employee_id, first_name, last_name, title, birth_date, notes) VALUES ({placeholders});")
        cursor.execute(insert_query, values)

with open(customers_data, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Пропускаем заголовки
    for row in reader:
        values = row
        placeholders = ', '.join(['%s' for _ in range(len(values))])
        insert_query = sql.SQL(f"INSERT INTO {customers} (customer_id, company_name, contact_name) VALUES ({placeholders});")
        cursor.execute(insert_query, values)

with open(orders_data, 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Пропускаем заголовки
    for row in reader:
        values = row
        placeholders = ', '.join(['%s' for _ in range(len(values))])
        insert_query = sql.SQL(f"INSERT INTO {orders} (order_id, customer_id, employee_id, order_date, ship_city) VALUES ({placeholders});")
        cursor.execute(insert_query, values)


# Подтверждение изменений и закрытие соединения
conn.commit()
cursor.close()
conn.close()