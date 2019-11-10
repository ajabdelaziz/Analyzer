import csv
import os
from time import time

import psutil as psutil
import psycopg2
import json
import random
from io import StringIO

def mem_report():
    p = psutil.Process(os.getpid())
    return "Mem: {0:.2f}MB".format(
        p.memory_info_ex().rss / (1024.0 * 1024)
    )


"""
    words processed: 99999999, 
    time: 5040, 
    Mem: 367.62MB
"""
words = ()
with open('words.txt', 'r') as f1:
    words = tuple(json.load(f1).keys())

start = time()
n = 0
try:
    connection = psycopg2.connect(user="postgres",
                                  password="",
                                  host="127.0.0.1",
                                  port="32768",
                                  database="analyzer")
    cursor = connection.cursor()
    total = 100000000
    random_words = []
    for i in range(1, total):
        random_words.append(random.choice(words))
        n += 1
        print(f"{mem_report()}, {n}")
    print(f"words processed: {n}, time: {int(time() - start)}, {mem_report()}")
    cursor.executemany('INSERT INTO new_words (word) VALUES ( %(value)s )', [dict(value=word) for word in random_words])
    connection.commit()
except (Exception, psycopg2.Error) as error:
    if (connection):
        print("Failed to insert record into word table", error)
finally:
    #closing database connection.
    print(f"words processed: {n}, time: {int(time() - start)}, {mem_report()}")
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
