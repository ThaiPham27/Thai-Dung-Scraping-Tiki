from bs4 import BeautifulSoup
import requests
import sqlite3
import re
import pandas as pd
from time import sleep
from random import randint

conn = sqlite3.connect('tiki.db')
cur = conn.cursor()

def create_products_table():
    query = """ CREATE TABLE IF NOT EXISTS tiki_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        product_sku INTEGER
        product_name VARCHAR(255),
        data_id INTEGER,
        current_price INTEGER,
        product_brand VARCHAR(255),
        category_id INTEGER,
        product_link TEXT,
        page INTEGER,
        create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) """

    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print("ERROR BY CREATE TABLE", err)

def get_html(link):
    """ From URL return HTML code in the website.
    get_html(link)
    link: URL of the website, type: string """

    r = requests.get(link)

    soup = BeautifulSoup(r.text,'html.parser')

    return soup

class Product:
    """ Instead of using a function to do CRUD on database,
        creating a class Product is preferred
        attributes: product_id, product_sku, product_name, data_id, current_price, product_brand, category_id,
        product_link, page, create_at
        instance method: save_into_db()
    """

    def __init__(self,product_id,product_sku,product_name,current_price,data_id, product_brand,product_link, category_id = None, page = None):
        self.product_id = product_id
        self.product_sku = product_sku
        self.product_name = product_name
        self.current_price = current_price
        self.data_id = data_id
        self.product_brand = product_brand
        self.product_link = product_link
        self.category_id = category_id
        self.page = page

    def save_into_db(self):
        query = """ INSERT INTO tiki_products (product_id, product_sku, product_name, data_id, current_price, product_brand, category_id,product_link, page)
        VALUES (?,?,?,?,?,?,?,?,?) """

        val = (self.product_id, self.product_sku, self.product_name, self.current_price, self.data_id, self.product_brand, self.product_link, self.category_id, self.page)
        try:
            cur.execute(query, val)
            self.cat_id = cur.lastrowid
            conn.commit()
        except Exception as err:
            print("ERROR BY INSERT:", err)

def get_item_list(full_page_html):
    """ From HTML string, returns HTML that contains item list section on tiki website.
    Get all items in the section as a list.
    get_item_list(full_page_html)
    full_page_html: html of a full tiki webpages, type: string
     """
    item_list = full_page_html.find_all('div',{'class': 'product-item'})

    return item_list

def get_data(item_html, crawled_url_list):
    """ Get data from item_list HTML for selected data_column list.
    return product class object to insert into product table.
    get_data(item_html)
    item_html: HTML of each item in item_list, type: string
     """
    product_link = 'https://tiki.vn/' + item_html.a['href']

     # check whether the item is crawled or not

    if len(crawled_url_list) == 0 or product_link not in crawled_url_list:

        # get info
        product_id = item_html['data-seller-product-id']
        product_sku = item_html['product-sku']
        product_name = item_html['data-title']
        current_price = item_html['data-price']
        data_id = item_html['data-id']
        product_brand = item_html['data-brand']

        product_link = "https://tiki.vn" + item_html['href']

        result = Product(product_id,product_sku,product_name, data_id, current_price, product_brand, product_link)

    else:
        result = "crawled"

    return result

create_products_table()

# get all categories that are not crawled yet
category_nc = pd.read_sql_query(''' 
SELECT *
FROM categories
WHERE total_sub_category = 0
AND total_pages is NULL
 ''', conn)

# get the last category that is crawled
category_crawled = pd.read_sql_query('''
SELECT *
FROM categories
WHERE total_sub_category = 0
AND total_pages IS NOT NULL
ORDER BY Id DESC
LIMIT 1
'''
)

category_nc = pd.concat([category_crawled, cateogry_nc], sort = False)

category_url_list = category_nc['url'].toList()
category_id_list = category_nc['id'].toList()
category_total_pages_list = category_nc['total_pages'].toList()
category_total_product_list = category_nc['total_products'].toList()

for cat_link, cat_id, cat_pages, cat_products in zip(category_url_list, category_id_list, category_total_pages_list, category_total_product_list):

    number_of_products = 1

    if cat_pages == None or cat_pages == 0:
        page_number = 1
    else:
        page_number = cat_pages

tiki_link = cat_page +'&page='

while number_of_products > 0:
    try:
        full_html = get_html(tiki_link + str(page_number))
        
        # get item list from full_html
        item_list = get_item_list(full_html)
        number_of_products = len(item_list)
    
    except:

        # if connection is lost, retry after sleeping 5 seconds
        sleep(5)
        try:
            full_html = get_html(tiki_link + str(page_number))
        
            # get item list from full_html
            item_list = get_item_list(full_html)
            number_of_products = len(item_list)
        except:
            if page_number < 20:
                number_of_products = 1
                page_number += 1
            else:
                number_of_products = 0
            continue

# if the last page is reached, no product in item list
if number_of_products > 0:
    n = 0

    product = pd.read_sql_query(''' SELECT product_link FROM tiki_products WHERE category_id = {cat_id} ''',conn)

    crawled_url_list = product['product-link'].toList()

    # get all data
    for item_html in item_list:
        output = get_data(item_html, crawled_url_list)
        
        if output != 'crawled':
            output.category_id = cat_id
            output.page = page_number
            
            output.save_into_db()
            n += 1
    print(f'Category ID: {cat_id}. FInish crawling page {page_number} with {n} products')

    if n!= 0:
        if cat_products == None:
            cat_products = 0
        # continue adding number of products
        cat_products += n
        update_total_pages_products_categories(cat_id, page_number, cat_products)

page_number += 1

sleep(randint(1,5))

print('Finish crawling')

import os
os.system('shutdown /p /f')