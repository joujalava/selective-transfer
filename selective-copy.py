from sqlalchemy import create_engine
from collections import deque, defaultdict
from networkx import has_path
from psycopg import connect

from args import DATABASE_URL, PARSER, INITIAL_DATABASE_TABLE, INITIAL_COLUMN
from database import read_rows, upsert_rows
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
    # copy_info[table_name][foreign_key_column_name] = rows with these values in this foreign column need to be copied
    copy_info: dict[str,dict[str,list]] = defaultdict(lambda: {}, {INITIAL_DATABASE_TABLE: {INITIAL_COLUMN: ids}}, )
    copy_queue = deque([INITIAL_DATABASE_TABLE])

    while len(copy_queue) > 0:
        table_name = copy_queue.pop()
        foreign_key_columns = copy_info[table_name]

        if len(foreign_key_columns) > 0:
            source_connection = source_engine.raw_connection()
            rows, columns = read_rows(table_name, list(foreign_key_columns.keys()), list(foreign_key_columns.values()), source_connection)
            source_connection.close()
        else:
            rows = []

        copy_table = True
        copy_additional_rows = False
        child_table_copied = False

        for table, _, data in foreign_key_relation_graph.in_edges(table_name, data=True):
            if table in copy_info and len(copy_info[table]) >= len(foreign_key_relation_graph.edges(table)):
                child_table_copied = True
                source_connection = source_engine.raw_connection()
                rrows, columns = read_rows(table_name, [data["referenced_column"]], [copy_info[table][data["source_column"]]], source_connection)
                source_connection.close()
                if not all([rrow in rows for rrow in rrows]):
                    copy_additional_rows = True
                    if len(foreign_key_columns) == 0:
                        for primary_key in foreign_key_relation_graph.nodes[table_name]["primary_key"]:
                            foreign_key_columns[primary_key] = []
                    for foreign_key_column_name, ids in foreign_key_columns.items():
                        row_index = columns.index(foreign_key_column_name)
                        # add distinct values
                        ids.extend([rrow[row_index] for rrow in rrows if rrow[row_index] not in ids])
                    edges = foreign_key_relation_graph.edges(table_name)
                    if len(edges) > 0:
                        copy_table = False
                        for _, referenced_table in edges:
                            if referenced_table not in copy_queue:
                                copy_queue.append(referenced_table)
        if child_table_copied:
            if copy_additional_rows:
                if not copy_table:
                    continue
                source_connection = source_engine.raw_connection()
                rows, columns = read_rows(table_name, list(foreign_key_columns.keys()), list(foreign_key_columns.values()), source_connection)
                source_connection.close()
            elif set([edge[2]["source_column"] for edge in foreign_key_relation_graph.edges(table_name, data=True)]) == set(copy_info[table_name].keys()):
                upsert_rows(table_name, foreign_key_relation_graph.nodes[table_name]["primary_key"], columns, rows, destination_connection)
                continue

        
        if len(rows) <= 0:
            continue

        foreign_key_columns = foreign_key_relation_graph.edges(table_name, data=True)
        
        for _, referenced_table, data in foreign_key_columns:
            if referenced_table in copy_info:
                try:
                    column_index = columns.index(data["source_column"])
                    if all([row[column_index] in copy_info[table_name][data["source_column"]] for row in rows]):
                        continue
                    else:
                        copy_table = False
                        # add distinct values
                        copy_info[table_name][data["source_column"]].extend([row[column_index] for row in rows if row[column_index] not in copy_info[table_name][data["source_column"]]])
                        if referenced_table not in copy_queue:
                            copy_queue.append(referenced_table)
                        if table_name not in copy_queue:
                            copy_queue.appendleft(table_name)

                except ValueError:
                    pass
            elif has_path(foreign_key_relation_graph, referenced_table, INITIAL_DATABASE_TABLE) and not has_path(foreign_key_relation_graph, referenced_table, table_name):
                copy_table = False
            elif not data["source_column"] in copy_info[table_name]:
                column_index = columns.index(data["source_column"])
                copy_info[table_name][data["source_column"]] = [row[column_index] for row in rows]
                if referenced_table not in copy_queue:
                    copy_queue.append(referenced_table)
                if table_name not in copy_queue:
                    copy_queue.appendleft(table_name)
                copy_table = False
        if copy_table:
            upsert_rows(table_name, foreign_key_relation_graph.nodes[table_name]["primary_key"], columns, rows, destination_connection)
            for table, _, data in foreign_key_relation_graph.in_edges(table_name, data=True):
                if not data["source_column"] in copy_info[table]:
                    index = columns.index(data["referenced_column"])
                    copy_info[table][data["source_column"]] = [row[index] for row in rows]
                    if table not in copy_queue:
                        copy_queue.appendleft(table)
                else:
                    if table not in copy_queue:
                        copy_queue.append(table)
    destination_connection.commit()
    destination_connection.close()


if __name__== "__main__":
    main()
