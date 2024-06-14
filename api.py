from sql_QP import get_QP
import time
from flask import Flask, request, jsonify
from sqlalchemy import text
import pandas as pd

QP, ORACLE_TABLE = get_QP()
CONN = ORACLE_TABLE["connection"]
app = Flask(__name__)


def add_underscore_if_inprogress(text):
    if "inprogress" in text:
        text = text.replace("inprogress", "in_progress")
    if "in progress" in text:
        text = text.replace("in progress", "in_progress")
    return text

@app.route("/api/apex/query", methods=["POST"])
def text_to_sql_query():
    if "query" not in request.json:
        return jsonify({"error": "No query part in JSON payload"})
    try:
        query = add_underscore_if_inprogress(request.json["query"])

        response, intermediats = QP.run_with_intermediates(query=query)
        sql_query = (
            intermediats["sql_output_parser"].outputs["output"].replace("\n", " ")
        )
        result = CONN.execute(text(sql_query))
        table_data = result.fetchall()
        data = pd.DataFrame(table_data, columns=tuple(result.keys()))
        data = data.applymap(lambda x: None if pd.isna(x) else x)
        for col in data.select_dtypes(include="datetime").columns:
            data[col] = data[col].astype(str)
        data = data.to_dict(orient="records")
        res = {}
        res["summary"] = str(response).replace("assistant: ", "")
        res["query"] = sql_query
        res["data"] = data
    except Exception as e:
        return {"error": f"{e}"}
    return res


if __name__ == "__main__":
    app.run()
