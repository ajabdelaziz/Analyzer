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
    Achieved the following stats 
    words processed: 99999999, 
    time: 585, 
    Mem: 4425.14MB
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
    f = StringIO()
    total = 100000000
    for i in range(1, total):
        word = random.choice(words)
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([word])
        n += 1
    f.seek(0)
    cursor.copy_from(f, "words", "\t", columns=("word",))
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
