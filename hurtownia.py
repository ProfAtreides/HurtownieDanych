import random
import pandas as pd
from faker import Faker
import mysql.connector
from sqlalchemy import create_engine
import psycopg2

fake = Faker()

product_categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Sports']


def generate_product_name():
    return ' '.join(fake.words(nb=random.randint(2, 4))).title()


products = [
    {'id': i, 'name': generate_product_name(), 'category': random.choice(product_categories)}
    for i in range(1, 101)
]

df_products = pd.DataFrame(products)

sales = []
for i in range(1, 100001):
    product_id = random.randint(1, 100)
    date = fake.date_between(start_date='-2y', end_date='today')
    quantity = random.randint(1, 20)
    unit_price = round(random.uniform(5, 100), 2)
    sales.append({'id': i, 'product_id': product_id, 'date': date, 'quantity': quantity, 'unit_price': unit_price})

df_sales = pd.DataFrame(sales)


def create_and_load_mysql():
    db_config = {
        'user': 'root',
        'password': '12345',
        'host': 'localhost',
        'database': 'data_base'
    }

    conn = mysql.connector.connect(**db_config)
    c = conn.cursor()

    # Tworzenie tabel
    c.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(255) NOT NULL
    )
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        id INT AUTO_INCREMENT PRIMARY KEY,
        product_id INT,
        date DATE,
        quantity INT,
        unit_price FLOAT,
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    ''')

    # Dodawanie danych do tabeli products
    products_data = [
        (row['id'], row['name'], row['category']) for _, row in df_products.iterrows()
    ]
    c.executemany('INSERT INTO products (id, name, category) VALUES (%s, %s, %s)', products_data)

    # Dodawanie danych do tabeli sales
    sales_data = [
        (row['product_id'], row['date'], row['quantity'], row['unit_price']) for _, row in df_sales.iterrows()
    ]
    c.executemany('INSERT INTO sales (product_id, date, quantity, unit_price) VALUES (%s, %s, %s, %s)', sales_data)

    conn.commit()
    conn.close()


def load_data_from_mysql():
    db_config = {
        'user': 'root',
        'password': '12345',
        'host': 'localhost',
        'database': 'data_base'
    }

    conn = mysql.connector.connect(**db_config)
    df_products = pd.read_sql_query('SELECT * FROM products', conn)
    df_sales = pd.read_sql_query('SELECT * FROM sales', conn)
    conn.close()
    return df_products, df_sales


def load_data_to_postgresql(df_products, df_sales):
    db_config_postgres = {
        'user': 'your_postgres_username',
        'password': 'your_postgres_password',
        'host': 'your_postgres_host',
        'port': 'your_postgres_port',
        'database': 'your_postgres_database'
    }

    conn_str_postgres = f"postgresql://{db_config_postgres['user']}:{db_config_postgres['password']}@{db_config_postgres['host']}:{db_config_postgres['port']}/{db_config_postgres['database']}"
    engine_postgres = create_engine(conn_str_postgres)

    df_products.to_sql('products', engine_postgres, if_exists='replace', index=False)
    df_sales.to_sql('sales', engine_postgres, if_exists='replace', index=False)


def main():
    create_and_load_mysql()

    df_products, df_sales = load_data_from_mysql()

    load_data_to_postgresql(df_products, df_sales)


if __name__ == '__main__':
    main()
