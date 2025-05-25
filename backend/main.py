from typing import Union
from lark import Lark
from algoritmos.parser_sql import SQLTransformer,timed_execute_query,sql_grammar
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class SQLQuery(BaseModel):
    query: str

@app.get("/")
def read_root():
    return {"proyecto": "Proyecto 1 BD2"}

@app.post("/sql_parser")
def parser_sql(sql_query: SQLQuery):
    parser = Lark(sql_grammar, parser="lalr", start="start")
    transformer = SQLTransformer()

    tree = parser.parse(sql_query.query)
    parsed = transformer.transform(tree)

    if hasattr(parsed, "children") and parsed.children:
        parsed = parsed.children[0]
    if hasattr(parsed, "children") and parsed.children:
        parsed = parsed.children[0]

    return timed_execute_query(parsed)
