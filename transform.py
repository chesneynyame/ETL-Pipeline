from re import split
import psycopg2
import csv
import os
from dotenv import load_dotenv
from datetime import datetime

order_transactions = []

def load(reader):
        for line in reader:
            line['products_ordered'] = clean_data(line['products_ordered'])
            line['date_time'] = datetime.strptime(line['date_time'],"%d/%m/%Y %H:%M")
            
            order_transactions.append(dict(line))
        
        return

def clean_data(give_string):
    character_lst = []
    for i, c in enumerate(give_string):
        if c.isdigit():
            character_lst.append(i-2)
    
    char_indx = []
    for indx, character in enumerate(character_lst):
        if indx % 3 == 0:
            
            char_indx.append(character)
    
    repl_char = ","
    temp = list(give_string)
    for indx in char_indx:
        temp[indx] = repl_char
    res  = ''.join(temp)

    
    return res

    
def remove_customer_info():
    for orders in order_transactions:
        del(orders['customer_name'])
        del(orders['payment_info'])
        
def transform_order_string():
    for orders in order_transactions:
        clean_orders = orders['products_ordered']
        clean_first_char = clean_orders[0]      
        if clean_first_char == ',':
            clean_orders = ''.join(clean_orders.split(clean_first_char, 1))
            orders['products_ordered'] = clean_orders
    
    for order in order_transactions:
        if 'products_ordered' in order.keys():    
            order['products_ordered'] = order['products_ordered'].replace(" , ", ",")  
            order['products_ordered'] = order['products_ordered'].replace(", ", ",")        
    
    
    return order_transactions


def products_order_split(order_transactions):
    order_transactions_copy = order_transactions.copy()
    raw_order_transactions = []
    
    for order in order_transactions_copy:
        raw_orders = order['products_ordered']
        raw_order_transactions.append(raw_orders)
    
    
    raw_order_str = str(raw_order_transactions).strip('[]')
    raw_order_split = raw_order_str.split(",")
    
    split_order_transactions = [{}]
    
    for order in raw_order_split:
        if order == "":
            continue
       
        last_order = split_order_transactions[len(split_order_transactions)-1]    
        
        if 'product_name' in last_order:
            last_order['product_price'] = order
            split_order_transactions.append({})
            continue
        
        else:
            last_order['product_name'] = order
        continue    
    
    for order in split_order_transactions:
        if 'product_name' and 'product_price' in order.keys():    
            order['product_price'] = order['product_price'].replace("'", "")
            order['product_price'] = order['product_price'].replace(" ", "")
            order['product_price'] = order['product_price'].rstrip()
            order['product_price'] = order['product_price'].lstrip()
            
            order['product_name'] = order['product_name'].replace("'","")
            order['product_name'] = order['product_name'].lstrip()
            order['product_name'] = order['product_name'].rstrip()
    
    while {} in split_order_transactions:
        split_order_transactions.remove({})
    
    
    unique_split_order_transactions = [dict(t) for t in {tuple(d.items()) for d in split_order_transactions}]
    
    
    return unique_split_order_transactions


def location_split(order_transactions):
    
    raw_locations = []
    
    for location in order_transactions:
        raw_loc = location['location']
        raw_locations.append(raw_loc)

    
    raw_location_str = str(raw_locations).strip('[]')
    raw_locations_split = raw_location_str.split(",")

    split_locations = [{}]
    
    for location in raw_locations_split:
        if location == "":
            continue
        
        last_location = split_locations[len(split_locations) - 1]
        
        last_location['location_name'] = location
        continue
    
    for location in split_locations:
        if 'location_name' in location.keys():    
            location['location_name'] = location['location_name'].replace("'","")
            location['location_name'] = location['location_name'].lstrip()
    

    return split_locations


def payment_type_split(order_transactions):
    
    raw_payment_type = []
    
    for payment_type in order_transactions:
        raw_payment_ops = payment_type['payment_type']
        raw_payment_type_dct = {}
        raw_payment_type_dct['payment_type'] = raw_payment_ops
        raw_payment_type.append(raw_payment_type_dct)
    
    
    unique_split_payment_type = [dict(t) for t in {tuple(d.items()) for d in raw_payment_type}]
    
    return unique_split_payment_type


def transaction_split():
    order_transactions_copy = order_transactions.copy()
    order_transactions_new = []
    
    
    
    for transactions in order_transactions_copy:
        prd_ordered = transactions['products_ordered']
        prd_split = prd_ordered.split(",")
        
        
        prd_list = []
        price_list = []
        
        for indx, x in enumerate(prd_split):
            if indx % 2 == 0:
                prd_list.append(x)
                
            elif indx % 2 == 1:
                price_list.append(x)
                
        
        prc_indx = 0
        
        for prd in prd_list:
            orders_dct = {}
            orders_dct['id'] = transactions['id']
            orders_dct['date_time'] = transactions['date_time']
            orders_dct['location'] = transactions['location']
            orders_dct['product_name'] = prd
            orders_dct['product_price'] = price_list[prc_indx]
            orders_dct['payment_type'] = transactions['payment_type']
            prc_indx = prc_indx + 1
            order_transactions_new.append(orders_dct)    
             
    
    for order in order_transactions_new:
        if 'product_name' and 'product_price' in order.keys():    
            order['product_price'] = order['product_price'].replace("'", "")
            order['product_price'] = order['product_price'].replace(" ", "")
            order['product_price'] = order['product_price'].rstrip()
            order['product_price'] = order['product_price'].lstrip()
            
            
            order['product_name'] = order['product_name'].replace("'","")
            order['product_name'] = order['product_name'].lstrip()
            order['product_name'] = order['product_name'].rstrip()   
    return order_transactions_new 
