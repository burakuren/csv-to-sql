import pandas as pd
from sqlalchemy import create_engine, update, delete, insert
from sqlalchemy.orm import Session
import requests
from django.http import JsonResponse

engine = create_engine('sqlite:///data.db', echo = True)
session = Session(engine)

csv_file = "~/proje/mysite/mysite/data.csv"

table_name = "data"
df = pd.read_csv(csv_file)

#Put it into db

def create_table(request):
    df.to_sql(table_name, con=engine, schema=None, if_exists='append', index=False, index_label=None, chunksize=None,
              dtype=None, method=None)
    return JsonResponse({"message": "Success"})


def read_table(request): # or csv file is gonna be transferred by filesystem.
    result = engine.execute(f"SELECT * FROM {table_name}").fetchall()

    return JsonResponse({"message":"Success"})

#There is gonna be a POST request and where are gonna use that json to update assosicated columns and rows.
def update_table(request):
    json_data = request.data

    # SQL EXECUTIONS
    engine.execute()
    session.commit()

    return JsonResponse({"message": "Success"})

def insert_value(request):
    json_data = request.data

    # SQL EXECUTIONS
    engine.execute()
    session.commit()
    return JsonResponse({"message":"Success"})


def delete_table():

    delete()