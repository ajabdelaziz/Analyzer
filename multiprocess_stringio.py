import csv
import os
import time

import psutil as psutil
import psycopg2
import json
import random
from io import StringIO
from multiprocessing import Process, Queue, current_process, cpu_count
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
    except (Exception, psycopg2.Error) as error:
        if (connection):
            print("Failed to insert record into word table", error)
    finally:
        # closing database connection.
        if(connection):
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
    number_of_processes = cpu_count() - 1
    tasks_to_accomplish = Queue()
    tasks_that_are_done = Queue()
    processes = []

    for i in range(number_of_task):
        tasks_to_accomplish.put(load_words_into_postgres)

    # creating processes
    for w in range(number_of_processes):
        p = Process(target=do_job, args=(tasks_to_accomplish, tasks_that_are_done))
        processes.append(p)
        p.start()

    # completing process
    for p in processes:
        print('completed')
        p.join()

    # print the output
    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())

    return True


if __name__ == '__main__':
    main()
