import src.create_db as create_db
import src.transform as transform
import src.load_db as load_db

def extract_transform_load(reader):
    
        
    ##Creates tables in postgress database
    tables = create_db.create_tables()
    tables.create_cafe_location_table()
    tables.create_products_table()
    tables.create_payment_type_table()
    tables.create_transactions_table()
    tables.create_basket_table()

    ##loads dataset from csv
    transform.load(reader)
        
    ##Removes sensitives information
    transform.remove_customer_info()

    ##Transforms order string
    transform.transform_order_string()
    
    #Loads clean data into database
    load_tables = load_db.load_tables()
    load_tables.load_products()
    load_tables.load_locations()
    load_tables.load_payment_type()
    load_tables.load_transactions()
    
    #Pulls transaction ID from transactions table in database
    load_tables.get_transaction_ids()
    
    ##Split orders per transaction ID
    transform.transaction_split()
    
    #Loads split orders
    load_tables.load_basket()

