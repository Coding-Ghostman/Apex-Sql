from llama_index.core.objects import (
    SQLTableNodeMapping,
    ObjectIndex,
    SQLTableSchema,
)
from llama_index.core import (
    VectorStoreIndex,
)


def get_schema_and_retreiver(sql_database, table_infos):
    table_node_mapping = SQLTableNodeMapping(sql_database)
    table_schema_objs = [
        SQLTableSchema(table_name=t.table_name, context_str=t.table_summary)
        for t in table_infos
    ]  # add a SQLTableSchema for each table

    obj_index = ObjectIndex.from_objects(
        table_schema_objs,
        table_node_mapping,
        VectorStoreIndex,
    )
    obj_retriever = obj_index.as_retriever(similarity_top_k=3)

    return table_schema_objs, obj_retriever
