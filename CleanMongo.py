import json
import ssl
import sys

from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
import boto3
import gridfs
import bson.json_util
import pandas as pd
from itertools import chain


def connect_to_postgresql(postgres_host, database, user, postgres_password, port):
    conn = psycopg2.connect(host=postgres_host, port=port, database=database, user=user, password=postgres_password)
    return conn


def query_postgresql(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return results


def connect_to_mongodb(host, port, username, password, db_name):
    client = MongoClient(host, port, username=username, password=password, tls=True, tlsAllowInvalidCertificates=True,
                         directConnection=True)
    # client = MongoClient(host, port)
    # client = MongoClient(host=mongo_uri, ssl_cert_reqs=ssl.CERT_NONE)
    # print(client.server_info())
    db = client[db_name]
    return db


def execute_query(db, collection_name, query):
    collection = db[collection_name]
    result = collection.find(query)
    return result


if __name__ == "__main__":

    # PostgreSQL connection params

    postgresql_host = "localhost"
    postgresql_port = "7000"
    postgresql_database = "db"
    postgresql_user = "user"
    postgresql_password = "psw"


    # Queries which needs to be executed on the Postgres DB

    postgresql_query = ("SELECT id FROM table WHERE status = 'Finalized' and "
                        "creation_date < '2023-12-01'")

    postgresql_query2 = "SELECT id FROM table WHERE creation_date < '2023-03-01';"


    # Connect to PostgreSQL

    postgres_conn = connect_to_postgresql(postgresql_host, postgresql_database, postgresql_user, postgresql_password,
                                          postgresql_port)
    postgres_result = query_postgresql(postgres_conn, postgresql_query)

    print("first postgres query done")

    postgres_result2 = query_postgresql(postgres_conn, postgresql_query2)

    print("second postgres query done")


    arr_postgres = []
    for doc in postgres_result:
        arr_postgres.append(doc[0])

    arr_postgres2 = []
    for doc in postgres_result2:
        arr_postgres2.append(doc[0])

    # MongoDB connection

    host = "localhost"
    port = 27017
    username = "user"
    password = "psw"
    db_name = "db"
    
    db = connect_to_mongodb(host, port, username, password, db_name)

    # The first input parameter represents the collection name where the queries will be executed
    print("Cleaning collection: " + sys.argv[1])
    collection = db[sys.argv[1]]

    complete_filtered_list = []
    first_query = [x for x in
                   collection.find({}, {"_id": 1, "templateId": 1, "revision": 1}).sort(
                       '_id', 1).limit(
                       1000)]
    last_doc_id = first_query[-1]['_id']
    complete_filtered_list.append(first_query)
    # The second input parameter represents the number of 1000 documents batches minus 1 needed to be looped
    # to get the entire dataframe where the queries will be executed 'int(sys.argv[2])'
    for x in range(int(sys.argv[2])):
        query_result = [x for x in
                        collection.find({"_id": {"$gt": last_doc_id}}, {"_id": 1, "templateId": 1, "revision": 1}).sort(
                            '_id', 1).limit(1000)]
        last_doc_id = query_result[-1]['_id']
        complete_filtered_list.append(query_result)

    print("end of initialization")
    df = pd.DataFrame(list(chain.from_iterable(complete_filtered_list)))

    dfFiltered = df[df['templateId'].isin(arr_postgres2)]

    resultDf = dfFiltered.loc[dfFiltered.groupby('templateId')['revision'].idxmax()]

    maxRows = resultDf['_id'].values.tolist()

    print("Max revisions Ids extracted...")

    #Deletion query
    
    resultDel1 = collection.delete_many(
        {
            "$and": [
                {'_id': {'$nin': maxRows}},
                {"templateId": {"$in": arr_postgres2}}
            ]
        }
    )

    print(f'Deleted {resultDel1} documents.')
    
    #Ids to delete extraction query
    """second_result = collection.find(
        {
            "$and":
                [
                    {"templateId": {"$in": arr_postgres2}},
                    {"_id": {"$nin": maxRows}}

                ]

        },
        {"_id": 1, "templateId": 1}
    )

    toDelete = []
    for doc in second_result:
        toDelete.append(doc['_id'])


    print("Documents to delete array ready")"""


    # END FIRST DELETION


    resultDel3 = collection.delete_many({"templateId": {"$in": arr_postgres}})

    print(f'Deleted {resultDel3} documents.')

    """third_query_result = list(collection.find(
        {
            "$and":
                [
                    {"templateId": {"$in": arr_postgres}}

                ]

        },
        {"_id": 1}
    ))

    toDelete3 = []
    for doc in third_query_result:
        toDelete3.append(doc['_id'])

    print("END")"""

