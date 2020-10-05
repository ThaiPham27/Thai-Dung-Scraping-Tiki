from bs4 import BeautifulSoup
import requests
import sqlite3
import re
import pandas as pd
from time import sleep
from random import randint

conn = sqlite3.connect('tiki.db')
cur = conn.cursor()

cur.execute('SELECT * FROM categories').fetchall()