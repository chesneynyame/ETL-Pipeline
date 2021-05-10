import psycopg2
import os
from dotenv import load_dotenv


load_dotenv()
database = os.environ["DB"]
host = os.environ["HOST"]
port = os.environ["PORT"]
user = os.environ["DB_USER"]
password = os.environ["PASSWORD"]

class Connection():
    def __init__(self):
        self.conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port)
    
        self.cursor = self.conn.cursor()

    def print(self):
        print("Database opened successfully", self.cursor)

class create_tables(Connection):
    def __init__(self):
        super().__init__()
        
    def create_cafe_location_table(self):
        sql = "CREATE TABLE IF NOT EXISTS cafe_location (location_id INT IDENTITY(1, 1), location_name VARCHAR(250) NOT NULL, PRIMARY KEY(location_id));"
        self.cursor.execute(sql)
        self.conn.commit()

        
    def create_products_table(self):
        sql = "CREATE TABLE IF NOT EXISTS products (product_id INT IDENTITY(1, 1), product_name VARCHAR(250) NOT NULL, product_price DECIMAL(6,2) NOT NULL, PRIMARY KEY(product_id));"
        self.cursor.execute(sql)
        self.conn.commit()

    
    def create_payment_type_table(self):
        sql = "CREATE TABLE IF NOT EXISTS payment_type (payment_id INT IDENTITY(1, 1), payment_type VARCHAR(250) NOT NULL, PRIMARY KEY(payment_id));"
        self.cursor.execute(sql)
        self.conn.commit()

        
    def create_transactions_table(self):
        sql = "CREATE TABLE IF NOT EXISTS transactions (transaction_id INT IDENTITY(1, 1), time_stamp TIMESTAMP NOT NULL, location_id INT NOT NULL, payment_type INT NOT NULL, transaction_total DECIMAL(6,2) NOT NULL, PRIMARY KEY(transaction_id), FOREIGN KEY (location_id) REFERENCES cafe_location (location_id), FOREIGN KEY (payment_type) REFERENCES payment_type (payment_id));"
        self.cursor.execute(sql)
        self.conn.commit()

    
    def create_basket_table(self):
        sql = "CREATE TABLE IF NOT EXISTS basket (transaction_id INT NOT NULL, product_id INT NOT NULL, FOREIGN KEY (transaction_id) REFERENCES transactions (transaction_id), FOREIGN KEY (product_id) REFERENCES products (product_id));"
        self.cursor.execute(sql)
        self.conn.commit()


