import pandas as pd
import duckdb
import psycopg2
import statistics
import timeit
import json

# чтение конфигурационного файла с запросами для бенчмарка
with open('config_file.json', 'r') as f:
    config = json.load(f)

# чтение csv файла с данными для тестирования
df = pd.read_csv(config["input_data_file"])

#########################################
# Выполнение запросов и измерение времени выполнения для PANDAS

def execute_query_pandas(query):
    query_times = []
    for _ in range(10):
        start_time = timeit.default_timer()
        result = df.groupby('VendorID').size().reset_index(name='counts')
        elapsed = timeit.default_timer() - start_time
        query_times.append(elapsed)
    median_time = statistics.median(query_times)
    return median_time


for query in config["queries"]:
    query_time = execute_query_pandas(query)
    print(f"Pandas Query: {query}")
    print(f"Median Time: {query_time} seconds")

#########################################


#########################################
# Выполнение запросов и измерение времени выполнения для DuckDB

con = duckdb.connect(database=':memory:')
con.register('data', df)

def execute_query_duckdb(query):
    query_times = []
    for _ in range(10):
        start_time = timeit.default_timer()
        result = con.execute(query).fetchdf()
        elapsed = timeit.default_timer() - start_time
        query_times.append(elapsed)
    median_time = statistics.median(query_times)
    return median_time

for query in config["queries"]:
    query_time = execute_query_duckdb(query)
    print(f"DuckDB Query: {query}")
    print(f"Median Time: {query_time} seconds")

#########################################


#########################################
# Выполнение запросов и измерение времени выполнения для SQLITE

def execute_query_sqlite(query):
    query_times = []
    for _ in range(10):
        start_time = timeit.default_timer()
        elapsed = timeit.default_timer() - start_time
        query_times.append(elapsed)
    median_time = statistics.median(query_times)
    return median_time

for query in config["queries"]:
    query_time = execute_query_sqlite(query)
    print(f"SQLite Query: {query}")
    print(f"Median Time: {query_time} seconds")

#########################################