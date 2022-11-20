import pandas as pd
from sqlalchemy.orm import Session
from django.http import JsonResponse
import sqlalchemy as db
import json
from rest_framework.decorators import api_view

engine = db.create_engine('url', echo=True)
session = Session(engine)
meta_data = db.MetaData(bind=engine)
db.MetaData.reflect(meta_data)

csv_file = "~/proje/mysite/mysite/scores_breakdown.csv"

df = pd.read_csv(csv_file)

#Put it into db
def create_table(request,table_name):

    table_name = table_name.split("=")[1]

    df.to_sql(table_name, con=engine, schema=None, if_exists='append', index=False, index_label=None, chunksize=None,
              dtype=None, method=None)

    engine.execute(
"""
ALTER TABLE data 
ADD COLUMN IF NOT EXISTS id VARCHAR;
""")
    session.commit()

    # TODO: If not genereated statement must be added!!!!
    engine.execute(
"""
ALTER TABLE data
ALTER COLUMN id DROP DEFAULT,
ALTER COLUMN id SET DATA TYPE UUID USING (uuid_generate_v4()), 
ALTER COLUMN id SET DEFAULT uuid_generate_v4();
    """)

    session.commit()

    return JsonResponse({"message": "Success"})


def read_table(request, table_name, id=None): # or csv file is gonna be transferred by filesystem.
    table_name = table_name.split("=")[1]
    table = meta_data.tables[table_name]
    if id == None:
        query = db.select(table)
    else:
        id = id.split("=")[1]
        query = db.select(table).where(table.columns.id == id)

    #If you wanna get results in any format you gotta do that here.
    result = engine.execute(query).fetchall()
    columns = []

    for column in table.columns :
        columns.append(column.name)

    object_d = []

    for row in result:
        d = {}
        for e in row:
            index = row.index(e)
            d[columns[index]] = e
        object_d.append(d)

    return JsonResponse({"message": "Success", "result": object_d})

#There is gonna be a POST request and where are gonna use that json to update assosicated columns and rows.
@api_view(["POST"])
def update_table(request,table_name):
    json_data = json.loads(request.body)
    table_name = table_name.split("=")[1]
    table = meta_data.tables[table_name]

    query = db.update(table).where(table.columns.id == json_data.get("id")).values(**json_data.get("values"))
    result = engine.execute(query)
    session.commit()

    return JsonResponse({"message": "Success"})

@api_view(["POST"])
def insert_value(request, table_name):

    json_data = json.loads(request.body)
    table_name = table_name.split("=")[1]
    table = meta_data.tables[table_name]

    query = db.insert(table).values(**json_data.get("values"))
    result = engine.execute(query)

    session.commit()

    return JsonResponse({"message": "Success"})

@api_view(["DELETE"])
def delete_row(request, table_name, id):
    table_name = table_name.split("=")[1]
    table = meta_data.tables[table_name]
    id = id.split("=")[1]
    query = db.delete(table).where(table.columns.id == id)
    result = engine.execute(query)
    session.commit()

    return JsonResponse({"message": "Success"})

def delete_table():
    None