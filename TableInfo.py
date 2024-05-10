from typing import List
from llama_index.core.program import LLMTextCompletionProgram
from llama_index.core.bridge.pydantic import BaseModel, Field
from llama_index.llms.openai import OpenAI
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import text
import openai
import json
import os

load_dotenv()
openai.api_key = os.environ.get("OPENAI_API_KEY")


class TableInfo(BaseModel):
    """Information regarding a structured table."""

    table_name: str = Field(
        ..., description="table name (must be underscores and NO spaces)"
    )
    table_summary: str = Field(
        ..., description="short, concise summary/caption of the table"
    )


prompt_str = """\
Give me a summary of the table with the following JSON format.

- Describe the table in a very conscise but precise way.
- Also indicate few of synonyms of the elements of the table if possible. (for example: 'usa' can be synonym of 'united states of america' or 'american', 'Female' can be synonym for 'woman' or only 'F') 

Table:
{table_str}

Synonyms in the table:
Summary: """


program = LLMTextCompletionProgram.from_defaults(
    output_cls=TableInfo,
    llm=OpenAI(model="gpt-3.5-turbo"),
    prompt_template_str=prompt_str,
)


def get_table_info(connection, include_tables) -> List[str]:
    """Generate the table related information for the given tables."""
    tableinfo_dir = "Test_Schema"
    table_infos = []
    if not os.path.exists(tableinfo_dir):
        os.makedirs(tableinfo_dir)
    for _, i in enumerate(include_tables):
        engine_executor = connection.execute(text(f"SELECT * FROM {i}"))
        df = DataFrame(engine_executor.fetchall())
        df.columns = engine_executor.keys()
        try:
            df = df.drop("degree", axis=1)
            df = df.drop("passport", axis=1)
            df = df.drop("photo_image", axis=1)
        except:  # noqa: E722
            pass
        df_str = df.head(10).to_csv()
        table_info = program(
            table_str=df_str,
        )
        table_info.table_name = i
        table_name = i
        print(f"Processed table: {table_name}")
        table_infos.append(table_info)
        out_file = f"{tableinfo_dir}/{_}_{table_name}.json"

        json.dump(table_info.dict(), open(out_file, "w"))

    return table_infos
