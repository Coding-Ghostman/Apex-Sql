from flask import Flask, request, jsonify
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from utils import db_Connect_thinModePool, remove_after_conf, remove_after_note
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv
import json
import os
load_dotenv()

app = Flask(__name__)
DATABASE_SETTINGS = {"db_user": "TEST_SCHEMA",
                     "db_pass": "Conneq_schema1",
                     "db_dsn": "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.ap-mumbai-1.oraclecloud.com))(connect_data=(service_name=ge39e7b01ee1b6f_connetqdevdb_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))",
                     "db_min": 1,
                     "db_max": 5,
                     "db_inc": 1}


user = DATABASE_SETTINGS["db_user"]
password = DATABASE_SETTINGS["db_pass"]
dsn = DATABASE_SETTINGS["db_dsn"]
min = DATABASE_SETTINGS["db_min"]
max = DATABASE_SETTINGS["db_max"]
inc = DATABASE_SETTINGS["db_inc"]



db = None
try:
    db = db_Connect_thinModePool(
        user=user, password=password, dsn=dsn, min=min, max=max, increment=inc)
except:
    print("Error in DB Connection")


@app.route('/api/apex/query', methods=['POST'])
def upload_file():
    llm = ChatOpenAI(model="gpt-3.5-turbo-0125", temperature=0,
                     api_key=os.getenv("OPENAI_API_KEY"))
    schema = """"""
    # check if the post request has the file part
    if 'query' not in request.json:
        return jsonify({'error': 'No query part in JSON payload'})
    schema = schema+request.json["ddl"]
    print(schema)
    table = ""
    try:
        with db.connect() as conn:
            table = conn.execute(text("SELECT * FROM TEST_SCHEMA.RESOURCES"))
    except Exception as e:
        print(f"Error: {e}")

    table_mark = pd.DataFrame(
        table.fetchall(), columns=tuple(table.keys())).to_markdown()

    template = """
    You are a Database Expert at a company. Your work is to provide only SQL Query about the company's database, to the user's Questions who is interacting with you.
    
    Write only the SQL query and nothing else. Do not wrap the SQL query in any other text, not even backticks. No other addition to the query like notes and stuff. JUST PROVIDE THE SQL QUERY.
    
    Always try to provide all the columns as possible in the sql query.
    Use the Table Schema below for answering the SQL Query. 
    
    Don't Give Escape characters in the query

    <SCHEMA>{schema}</SCHEMA>
    
    <TABLE>{table}</TABLE>
    
    FEW EXAMPLES HAVE BEEN GIVEN TO YOU:
    
    USER_QUESTION: selects resource name who has has apex skill
    SQL_QUERY: SELECT * FROM resources WHERE (DBMS_LOB.INSTR(Skills, 'APEX') > 0 AND DBMS_LOB.INSTR(Skills, 'APEX,AI') = 0) OR (DBMS_LOB.INSTR(Skills, ',APEX') > 0 AND DBMS_LOB.INSTR(Skills, ',APEX,AI') = 0) OR (DBMS_LOB.INSTR(Skills, 'APEX,') > 0 AND DBMS_LOB.INSTR(Skills, 'APEX,AI') = 0) OR (DBMS_LOB.INSTR(Skills, ',APEX,') > 0 AND DBMS_LOB.INSTR(Skills, ',APEX,AI') = 0);

    USER_QUESTION: selects resource name who has has AI skill
    SQL_QUERY: SELECT * FROM resources WHERE DBMS_LOB.INSTR(Skills, 'AI') > 0 AND (DBMS_LOB.INSTR(Skills, 'APEX,AI') = 0 OR DBMS_LOB.INSTR(Skills, 'APEX,AI') > DBMS_LOB.INSTR(Skills, 'AI'));

    USER_QUESTION: selects resource name who has has both APEX and AI skill
    SQL_QUERY: SELECT * FROM resources WHERE DBMS_LOB.INSTR(Skills, 'APEX') > 0 AND DBMS_LOB.INSTR(Skills, 'AI') > 0 AND (DBMS_LOB.INSTR(Skills, 'APEX,AI') > 0 OR DBMS_LOB.INSTR(Skills, 'AI,APEX') > 0);
    
    Your turn:
    USER_QUESTION: {question}
    SQL_QUERY:
    """
    prompt = ChatPromptTemplate.from_template(template)
    sqlchain = prompt | llm | StrOutputParser()

    query = sqlchain.invoke(
        {"schema": schema, "question": request.json["query"], "table": table_mark})
    query = query.replace("\\", "").replace(";", "")
    query = remove_after_conf(remove_after_note(query))

    result = ""
    try:
        with db.connect() as conn:
            result = conn.execute(text(query))
    except Exception as e:
        print(f"Error: {e}")
    if result:
        table_data = result.fetchall()
        table_data_markdown = pd.DataFrame(
            table_data, columns=tuple(result.keys())).to_markdown()
        data = pd.DataFrame(table_data, columns=tuple(
            result.keys())).to_dict("records")

    llm = ChatOpenAI(model="gpt-3.5-turbo-0125", temperature=0,
                     api_key=os.getenv("OPENAI_API_KEY"), model_kwargs={"response_format": {"type": "json_object"}})
    template = """
    PROVIDE THE ANSWER WITHIN MAX 100 WORDS.!!
    You are a data analyst at a company. You are interacting with a user who is asking you questions about the company's database.
    Based on the Table Schema, Conversation History, SQL Query and SQL Response, ANSWER LIKE THE BELOW JSON STRUCTURE, MAKE THE KEY AS "Insights and Analysis" and provide the three Insights in List formay:
    
        <curlybraces start>
            Insights and Analysis:[Give atleast 5 Insights and Analysis about the SQL Response. Each of the insights will be in an individual string in an array]
        <curlybraces end>

    <SCHEMA>{schema}</SCHEMA>

    SQL Query: <SQL>{query}</SQL>
    User question: {question}
    SQL Response:
    ```
    {response}
    ```

    Short analysis:

    """

    prompt = ChatPromptTemplate.from_template(template)

    summary_chain = prompt | llm | StrOutputParser()
    summary = summary_chain.invoke(
        {"schema": schema, "question": request.json["query"], "query": query, "response": table_data_markdown})
    summary = summary.replace("\n", "").replace("\t", "").replace("\\", "")

    print({"query": query, "data": data, "summary": json.loads(summary)})
    return {"query": query, "data": data, "summary": json.loads(summary)}


if __name__ == '__main__':
    app.run(debug=True)
