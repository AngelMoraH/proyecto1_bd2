from typing import Union
#from lark import Lark
import time
from algoritmos.parser_sql import SQLTransformer,timed_execute_query,sql_grammar
from fastapi import FastAPI
from pydantic import BaseModel
from algoritmos.text_index.query import build_search
app = FastAPI()

class SQLQuery(BaseModel):
    query: str
    top_k: int

@app.get("/")
def read_root():
    return {"proyecto": "Proyecto 1 BD2"}

"""@app.post("/sql_parser")
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

"""
@app.post("/search_text")
def search_text(query: SQLQuery):
    start_time = time.time()
    response = build_search(query.query,query.top_k)
    end_time = time.time()
    elapsed = (end_time - start_time) * 1000
    
    print({"result": response, "execution_time_ms": round(elapsed, 3)})

    return {"result": response, "execution_time_ms": round(elapsed, 3)}