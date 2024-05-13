from config import read_config
from utils import db_Connect_thinModePool
from TableInfo import get_table_info
from table_schema import get_schema_and_retreiver
from index_tables import index_all_tables
import os
from typing import List

import llama_index
import openai
from dotenv import load_dotenv
from llama_index.core import (
    PromptTemplate,
    SQLDatabase,
)
from llama_index.core.llms import ChatResponse
from llama_index.core.objects import (
    SQLTableSchema,
)
from llama_index.core.prompts.default_prompts import DEFAULT_TEXT_TO_SQL_PROMPT
from llama_index.core.query_pipeline import (
    FnComponent,
)
from llama_index.core.retrievers import SQLRetriever
from llama_index.llms.openai import OpenAI
from llama_index.core.query_pipeline import (
    QueryPipeline as QP,
    InputComponent,
)

load_dotenv()
openai.api_key = os.environ.get("OPENAI_API_KEY")
config = read_config()
oracle_table = db_Connect_thinModePool(config)


def get_table_context_str():
    """Get table context string."""
    global oracle_table
    context_strs = []
    if not oracle_table["connection"].is_healthy():
        config = read_config()
        oracle_table = db_Connect_thinModePool(config)
    for table_schema_obj in table_schema_objs:
        table_info = sql_database.get_single_table_info(table_schema_obj.table_name)
        if table_schema_obj.context_str:
            table_opt_context = " The table description is: "
            table_opt_context += table_schema_obj.context_str
            table_info += table_opt_context

        context_strs.append(table_info)
    return "\n\n".join(context_strs)


def parse_response_to_sql(response: ChatResponse) -> str:
    """Parse response to SQL."""
    global oracle_table
    if not oracle_table["connection"].is_healthy():
        config = read_config()
        oracle_table = db_Connect_thinModePool(config)
    response = response.message.content.replace("", "")
    sql_query_start = response.find("SQLQuery:")
    if sql_query_start != -1:
        response = response[sql_query_start:]
        if response.startswith("SQLQuery:"):
            response = response[len("SQLQuery:") :]
    sql_result_start = response.find("SQLResult:")
    if sql_result_start != -1:
        response = response[:sql_result_start]
    response.replace("\n", " ")
    return response.strip().strip("```").strip().replace("\n", " ").replace(";", "")


def get_table_context_and_rows_str(
    query_str: str, table_schema_objs: List[SQLTableSchema]
):
    """Get table context string."""
    if not oracle_table["connection"].is_healthy():
        config = read_config()
        oracle_table = db_Connect_thinModePool(config)

    context_strs = []
    for table_schema_obj in table_schema_objs:
        # first append table info + additional context
        table_info = sql_database.get_single_table_info(table_schema_obj.table_name)
        if table_schema_obj.context_str:
            table_opt_context = "The table description is: "
            table_opt_context += table_schema_obj.context_str
            table_info += table_opt_context

        # also lookup vector index to return relevant table rows
        vector_retriever = vector_index_dict[table_schema_obj.table_name].as_retriever(
            similarity_top_k=2
        )
        relevant_nodes = vector_retriever.retrieve(query_str)
        if len(relevant_nodes) > 0:
            table_row_context = "\nHere are some relevant example rows (values in the same order as columns above)\n"
            for node in relevant_nodes:
                table_row_context += str(node.get_content()) + "\n"
            table_info += table_row_context

        context_strs.append(table_info)
    return "\n\n".join(context_strs)


def get_QP():
    qp = QP(
        modules={
            "input": InputComponent(),
            "table_retriever": QP_components["table_retriever"],
            "table_output_parser": QP_components["table_output_parser"],
            "text2sql_prompt": QP_components["text2sql_prompt"],
            "text2sql_llm": QP_components["text2sql_llm"],
            "sql_output_parser": QP_components["sql_output_parser"],
            "sql_retriever": QP_components["sql_retriever"],
            "response_synthesis_prompt": QP_components["response_synthesis_prompt"],
            "response_synthesis_llm": QP_components["response_synthesis_llm"],
        },
        verbose=True,
    )
    qp.add_link("input", "table_retriever")

    qp.add_link("input", "table_output_parser", dest_key="query_str")
    qp.add_link("table_retriever", "table_output_parser", dest_key="table_schema_objs")
    qp.add_link("input", "text2sql_prompt", dest_key="query_str")
    qp.add_link("table_output_parser", "text2sql_prompt", dest_key="schema")
    qp.add_chain(
        ["text2sql_prompt", "text2sql_llm", "sql_output_parser", "sql_retriever"]
    )
    qp.add_link("sql_output_parser", "response_synthesis_prompt", dest_key="sql_query")
    qp.add_link("sql_retriever", "response_synthesis_prompt", dest_key="context_str")
    qp.add_link("input", "response_synthesis_prompt", dest_key="query_str")
    qp.add_link("response_synthesis_prompt", "response_synthesis_llm")
    return qp, oracle_table


INCLUDE_TABLES = ["visa_requests2"]
llm = OpenAI(model="gpt-3.5-turbo", response_format={"type": "json_object"})
table_infos = get_table_info(oracle_table["connection"], INCLUDE_TABLES)
sql_database = SQLDatabase(
    oracle_table["engine"], schema="test_schema", include_tables=INCLUDE_TABLES
)

table_schema_objs, obj_retriever = get_schema_and_retreiver(sql_database, table_infos)


sql_retriever = SQLRetriever(sql_database)
table_parser_component = FnComponent(fn=get_table_context_str)
sql_parser_component = FnComponent(fn=parse_response_to_sql)
table_parser_component = FnComponent(fn=get_table_context_and_rows_str)
text2sql_prompt = DEFAULT_TEXT_TO_SQL_PROMPT.partial_format(
    dialect=oracle_table["engine"].dialect.name
)
vector_index_dict = index_all_tables(sql_database)


response_synthesis_prompt_str = (
    "Given an input question, synthesize a response from the query results.\n"
    "\nExtra Notes: If You are engaging with table related to visas, if possible, do not add these columns with the name 'degree', 'passport', 'photo_image\n"
    "Query: {query_str}\n"
    "SQL: {sql_query}\n"
    "SQL Response: {context_str}\n"
    "Response: "
)
response_synthesis_prompt = PromptTemplate(
    response_synthesis_prompt_str,
)

QP_components = {
    "table_retriever": obj_retriever,
    "table_output_parser": table_parser_component,
    "text2sql_prompt": text2sql_prompt,
    "text2sql_llm": llm,
    "sql_output_parser": sql_parser_component,
    "sql_retriever": sql_retriever,
    "response_synthesis_prompt": response_synthesis_prompt,
    "response_synthesis_llm": llm,
}
