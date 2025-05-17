from typing import Union
from lark import Lark
from algoritmos.parser_sql import SQLTransformer,execute_query,sql_grammar,images_table
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class SQLQuery(BaseModel):
    query: str

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/sql_parser")
def parser_sql(sql_query: SQLQuery):
    parser = Lark(sql_grammar, parser="lalr", start="start")
    transformer = SQLTransformer()

    tree = parser.parse(sql_query.query)
    parsed = transformer.transform(tree)

    # Desempaquetar el árbol hasta llegar al dict real
    if hasattr(parsed, "children") and parsed.children:
        parsed = parsed.children[0]
        if hasattr(parsed, "children") and parsed.children:
            parsed = parsed.children[0]

    result = execute_query(parsed, images_table)
    return {"result": result}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}