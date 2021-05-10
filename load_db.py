from dotenv import load_dotenv
import os
import psycopg2
import src.transform as transform


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

class load_tables(Connection):
    def __init__(self):
        super().__init__()

    def load_products(self):
        load_products = transform.products_order_split(transform.order_transactions)
        self.cursor.executemany("""INSERT INTO products (product_name, product_price) VALUES (%(product_name)s, %(product_price)s)""", load_products)
        self.conn.commit()
    
    def load_locations(self):
        load_locations = transform.location_split(transform.order_transactions)
        self.cursor.executemany("""INSERT INTO cafe_location (location_name) VALUES (%(location_name)s)""", load_locations)
        self.conn.commit()
        
    def load_payment_type(self):
        load_payment = transform.payment_type_split(transform.order_transactions)
        self.cursor.executemany("""INSERT INTO payment_type (payment_type) VALUES (%(payment_type)s)""", load_payment)
        self.conn.commit()
    
    def load_transactions(self):
        list_of_transactions = transform.order_transactions
        self.cursor.executemany("""INSERT INTO transactions (time_stamp, location_id, payment_type, transaction_total) VALUES (%(date_time)s, (SELECT location_id FROM cafe_location WHERE location_name = %(location)s LIMIT 1), (SELECT payment_id FROM payment_type WHERE payment_type = %(payment_type)s LIMIT 1), %(order_total)s)""", list_of_transactions)
        self.conn.commit()
    
    def get_transaction_ids(self):
        self.cursor.execute("""SELECT * FROM transactions""")
        row = self.cursor.fetchall()
        self.conn.commit()

        new_lst = []
        
        for r in row:
            transaction_id = {}
            transaction_id['id'] = r[0]
            new_lst.append(transaction_id)
        
        id_indx = 0
        
        for orders in transform.order_transactions:
            orders['id'] = new_lst[id_indx]['id']
            id_indx = id_indx +1
        

        return transform.order_transactions

    def load_basket(self):
        individual_transaction = transform.transaction_split()
        self.cursor.executemany("""INSERT INTO basket (transaction_id, product_id) VALUES (%(id)s, (SELECT product_id FROM products WHERE product_name = %(product_name)s LIMIT 1))""", individual_transaction)
        self.conn.commit()
