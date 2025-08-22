from psycopg import connection
from sys import exit
from itertools import chain
import traceback

def read_rows(table_name: str, column_names: list[str], column_values: list, connection: connection):
    print(f"Reading rows from {table_name} with column(s) {column_names} {column_values}")
    where_clauses = " OR ".join([f"{column_name} IN ({",".join(["%s"] * len(column_values[index]))})" for index, column_name in enumerate(column_names)])
    rows = []
    columns = []
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE {where_clauses}", list(chain.from_iterable(column_values)))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
    except:
        print(f"Error while reading from {table_name}")
        traceback.print_exc()
        exit(1)
    return rows, columns


def upsert_rows(table_name: str, primary_key_columns: list[str], column_names: list[str], rows: list, connection: connection):
    print(f"Upserting rows in {table_name} {[[row[column_names.index(primary_key_column)] for primary_key_column in primary_key_columns] for row in rows]}")
    columns = ",".join([f"(s).{column_name}" for column_name in column_names])
    where_clauses = " AND ".join([f"{table_name}.{column} = (s).{column}" for column in primary_key_columns])
    set_clauses = ",".join([f"{column} = (s).{column}" for column in column_names if column not in primary_key_columns])
    try:
        cursor = connection.cursor()
        cursor.executemany(f"MERGE INTO {table_name} USING (VALUES(({",".join(["%s"] * len(column_names))})::{table_name})) AS new (s) ON {where_clauses} WHEN MATCHED THEN UPDATE SET {set_clauses} WHEN NOT MATCHED THEN INSERT VALUES({columns})", rows)
        cursor.close()
    except:
        print(f"Error while upserting rows into {table_name}")
        traceback.print_exc()
        exit(1)

def delete_rows(table_name: str, primary_key_columns: list[str], primary_key_column_values, connection: connection):
    print(f"Deleting rows in {table_name} {primary_key_column_values}")
    where_clauses = " AND ".join([f"{column} = %s" for column in primary_key_columns])
    try:
        cursor = connection.cursor()
        cursor.executemany(f"DELETE FROM {table_name} WHERE {where_clauses}", primary_key_column_values)
        cursor.close()
    except:
        print(f"Error while upserting rows into {table_name}")
        traceback.print_exc()
        exit(1)