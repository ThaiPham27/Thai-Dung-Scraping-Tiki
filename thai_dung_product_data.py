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
        product_image_link TEXT,
        original_price INTEGER,
        discount_p FLOAT,
        rating_p FLOAT,
        number_of_reviews INTEGER,
        page INTEGER,
        create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) """

    try:
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print("ERROR BY CREATE TABLE", err)

def update_total_pages_products_categories(cat_id, total_pages, total_products):
    query = """
        UPDATE categories
        SET total_pages = ?,
            total_products = ?
        WHERE id = ?;
    """
    val = (total_pages, total_products, cat_id)
    try:
        cur.execute(query, val)
        conn.commit()
    except Exception as err:
        print('ERROR BY UPDATE CATEGORIES:', err)

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

    def __init__(self,product_id,product_sku,product_name,current_price,data_id, product_brand,product_link,product_image_link, original_price, discount_p, rating_p, number_of_reviews, item_id = None, category_id = None, page = None):
        self.product_id = product_id
        self.product_sku = product_sku
        self.product_name = product_name
        self.current_price = current_price
        self.data_id = data_id
        self.product_brand = product_brand
        self.product_link = product_link
        self.category_id = category_id
        self.page = page
        self.product_image_link = product_image_link
        self.original_price = original_price
        self.discount_p = discount_p
        self.rating_p = rating_p
        self.number_of_reviews = number_of_reviews


    def save_into_db(self):
        query = """ INSERT INTO tiki_products (product_id, 
        product_sku, 
        product_name, 
        data_id, 
        current_price, 
        product_brand, 
        category_id,
        product_link,
        original_price,
        product_image_lin,
        number_of_reviews,
        rating_p,
        discount_p, 
        page)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?) """

        val = (self.product_id, 
        self.product_sku, 
        self.product_name, 
        self.current_price, 
        self.data_id, 
        self.product_brand, 
        self.product_link, 
        self.category_id, 
        self.product_image_link,
        self.original_price,
        self.rating_p,
        self.discount_p,
        self.number_of_reviews,
        self.page)
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
        product_image_link = item_html.a.img['src']

        try:
            original_price = item_html.find('span', {'class':'price-regular'}).text
            original_price = int(re.sub(r'\.|đ', '', original_price))

            discount_p = item_html.find('span', {'class':'sale-tag sale-tag-square'}).text
            discount_p = float(re.sub(r'-|%', '', discount_p))
        except:
            original_price = current_price
            discount_p = 0

        # get number_of_reviews
        try:
            rating_section = item_html.find('div', {'class':'review-wrap'})
        
            number_of_reviews = rating_section.find('p', {'class':'review'}).text
            number_of_reviews = int(re.sub(r'\(| nhận xét\)', '', number_of_reviews))
        except:
            number_of_reviews = 0

        # getting to rating_pct
        try:
            p_rating = rating_section.find('p', {'class':'rating'})
            span_rating = p_rating.find('span', {'class':'rating-content'})
            
            rating_p = span_rating.span['style']
            rating_p = float(re.sub(r'width:|%', '', rating_p))
        except:
            rating_p = 0

        result = Product(product_id, product_sku, product_name, current_price, data_id, product_brand, product_link, product_image_link, original_price, discount_p, rating_p, number_of_reviews)

    else:
        result = "crawled"

    return result

create_products_table()

# get all categories that are not crawled yet
category = pd.read_sql_query(''' 
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
''',conn
)

category = pd.concat([category_crawled, category], sort=False)

df_category_url_list = category['url'].tolist()
df_category_id_list = category['id'].tolist()
df_category_total_pages_list = category['total_pages'].tolist()
df_category_total_products_list = category['total_products'].tolist()

for cat_link, cat_id, cat_pages, cat_products in zip(df_category_url_list, df_category_id_list, df_category_total_pages_list, df_category_total_products_list):

    number_of_products = 1

    if cat_pages == None or cat_pages == 0:
        page_number = 1
    else:
        page_number = cat_pages

tiki_link = cat_link +'&page='

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

# import os
# os.system('shutdown /p /f')