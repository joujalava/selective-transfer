from sqlalchemy import MetaData, Engine
from networkx import MultiDiGraph

def create_graph_from_dbs_foreign_key_relations(engine: Engine):
    metadata = MetaData()
    metadata.reflect(bind=engine)

    graph = MultiDiGraph()
    for table_name, table in metadata.tables.items():
        graph.add_node(table_name, primary_key = [column.name for column in table.primary_key.columns])
        for column in table.columns.values():
            for foreign_key in column.foreign_keys:
                graph.add_edge(table_name, foreign_key.column.table.name, source_column = column.name, referenced_column = foreign_key.column.name)
    return graph