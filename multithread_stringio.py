import csv
import os
import threading
import time
from multiprocessing import Queue

import psutil as psutil
import psycopg2
import json
import random
from io import StringIO
import queue # imported for using queue.Empty exception


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

with open('words.txt', 'r') as f1:
    words = tuple(json.load(f1).keys())


def load_words_into_postgres():
    start = time.time()
    n = 0
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="",
                                      host="127.0.0.1",
                                      port="32769",
                                      database="analyzer")
        cursor = connection.cursor()
        f = StringIO()
        total = 10000000
        for i in range(1, total):
            word = random.choice(words)
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([word])
            n += 1
        f.seek(0)
        cursor.copy_from(f, "multi_words", "\t", columns=("word",))
        connection.commit()
        time.sleep(.5)
    except (Exception, psycopg2.Error) as error:
        if (connection):
            print("Failed to insert record into word table", error)
    finally:
        # closing database connection.
        print(f"processed: {n} words, time: {int(time.time() - start)}, {mem_report()}")
        if (connection):
            cursor.close()
            connection.close()

def do_job(tasks_to_accomplish, tasks_that_are_done):
    while True:
        start = time.time()
        try:
            '''
                try to get task from the queue. get_nowait() function will 
                raise queue.Empty exception if the queue is empty. 
                queue(False) function would do the same task also.
            '''
            task = tasks_to_accomplish.get_nowait()
            task()
        except queue.Empty:

            break
        else:
            '''
                if no exception has been raised, add the task completion 
                message to task_that_are_done queue
            '''
            print(task)
            tasks_that_are_done.put(f"words processed: 10,000,000, time: {int(time.time() - start)}, {mem_report()}")
            time.sleep(.5)
    return True


def main():
    number_of_task = 10
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()
    threads = []

    for i in range(number_of_task):
        tasks_to_accomplish.put(load_words_into_postgres)

    # creating processes
    for i in range(number_of_task):
        thread = threading.Thread(target=do_job, name=f"thread-{i}", args=(tasks_to_accomplish, tasks_that_are_done))
        thread.start()
        threads.append(thread)

    # completing process
    for t in threads:
        print('completed')
        t.join()

    # print the output
    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())

    return True


if __name__ == '__main__':
    main()

