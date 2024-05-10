from llama_index.core import (
    VectorStoreIndex,
    load_index_from_storage,
    SQLDatabase,
    StorageContext,
)
from sqlalchemy import text
from llama_index.core.schema import TextNode
import os
from pandas import DataFrame
from pathlib import Path
from typing import Dict


def index_all_tables(
    sql_database: SQLDatabase, table_index_dir: str = "table_index_dir"
) -> Dict[str, VectorStoreIndex]:
    """Index all tables."""
    if not Path(table_index_dir).exists():
        os.makedirs(table_index_dir)

    vector_index_dict = {}
    engine = sql_database.engine
    for table_name in sql_database.get_usable_table_names():
        print(f"Indexing rows in table: {table_name}")
        if not os.path.exists(f"{table_index_dir}/{table_name}"):
            # get all rows from table
            with engine.connect() as conn:
                cursor = conn.execute(text(f"SELECT * FROM {table_name}"))
                result = cursor.fetchall()
                df = DataFrame(result)
                df.columns = cursor.keys()
            try:
                df = df.drop("degree", axis=1)
                df = df.drop("passport", axis=1)
                df = df.drop("photo_image", axis=1)
            except:  # noqa: E722
                pass
            row_tups = list(df.itertuples(index=False, name=None))
            # index each row, put into vector store index
            nodes = [TextNode(text=str(t)) for t in row_tups]

            # put into vector store index (use OpenAIEmbeddings by default)
            index = VectorStoreIndex(nodes)

            # save index
            index.set_index_id("vector_index")
            index.storage_context.persist(f"{table_index_dir}/{table_name}")
        else:
            # rebuild storage context
            storage_context = StorageContext.from_defaults(
                persist_dir=f"{table_index_dir}/{table_name}"
            )
            # load index
            index = load_index_from_storage(storage_context, index_id="vector_index")
        vector_index_dict[table_name] = index

    return vector_index_dict
