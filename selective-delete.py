from sqlalchemy import create_engine
from collections import deque, defaultdict, Counter
from itertools import chain
from psycopg import connect

from args import DATABASE_URL, PARSER, INITIAL_DATABASE_TABLE, INITIAL_COLUMN
from database import read_rows, delete_rows
from graph import create_graph_from_dbs_foreign_key_relations

def main():
    args = PARSER.parse_args()

    print("Connecting to source database")
    source_engine = create_engine(DATABASE_URL.replace("postgresql", "postgresql+psycopg") + args.source)
    print("Connecting to destination database")
    destination_connection = connect(DATABASE_URL + args.destination)

    foreign_key_relation_graph = create_graph_from_dbs_foreign_key_relations(source_engine)

    ids = args.ids.split(",")
    # Structure to store what rows need to be copied
    # read_info[table_name][foreign_key_column_name] = tuple with [0] for source and [1] for destination for rows with these values in this foreign column need to be copied
    read_info: dict[str,dict[str,tuple[list, list]]] = defaultdict(lambda: {}, {INITIAL_DATABASE_TABLE: {INITIAL_COLUMN: [ids, ids]}})
    read_queue = deque([INITIAL_DATABASE_TABLE])
    read_tables: list[str] = []
    extra_rows_in_dest: dict[str,list] = defaultdict(lambda: [])

    while len(read_queue) > 0:
        table_name = read_queue.pop()
        foreign_key_columns = read_info[table_name]

        if any([len(values[0]) > 0 for values in foreign_key_columns.values()]):
            source_connection = source_engine.raw_connection()
            source_rows, _ = read_rows(table_name, list(foreign_key_columns.keys()), [values[0] for values in foreign_key_columns.values()], source_connection)
            source_connection.close()
        else:
            source_rows = []
        
        dest_rows, columns = read_rows(table_name, list(foreign_key_columns.keys()), [values[1] for values in foreign_key_columns.values()], destination_connection)
        
        primary_keys = foreign_key_relation_graph.nodes[table_name]["primary_key"]
        
        primary_key_indexes = [columns.index(primary_key) for primary_key in primary_keys]
        source_row_primary_keys = [[source_row[primary_key_index] for primary_key_index in primary_key_indexes] for source_row in source_rows]
        dest_row_primary_keys = [[dest_row[primary_key_index] for primary_key_index in primary_key_indexes] for dest_row in dest_rows]
        extra_rows = list(chain.from_iterable(extra_rows_in_dest[table_name]))
        extra_rows_in_dest[table_name].append([dest_row_primary_key for dest_row_primary_key in dest_row_primary_keys if dest_row_primary_key not in source_row_primary_keys and dest_row_primary_key not in extra_rows])
        if len(extra_rows_in_dest[table_name][-1]) == 0 and table_name in read_tables:
            del extra_rows_in_dest[table_name][-1]
            continue

        read_tables.append(table_name)
        for table, _, data in foreign_key_relation_graph.in_edges(table_name, data=True):
            index = columns.index(data["referenced_column"])
            read_info[table][data["source_column"]] = ([row[index] for row in source_rows], [row[index] for row in dest_rows])
            if table not in read_queue:
                read_queue.appendleft(table)

    read_tables.reverse()
    deletes_left = Counter(read_tables)
    
    for table in read_tables:
        deletes_left[table] -= 1
        primary_keys = foreign_key_relation_graph.nodes[table]["primary_key"]
        delete_rows(table, primary_keys, extra_rows_in_dest[table][deletes_left[table]], destination_connection)
    destination_connection.commit()
    destination_connection.close()


if __name__== "__main__":
    main()
