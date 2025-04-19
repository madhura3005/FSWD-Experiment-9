import pymongo
import psycopg2

#connect to mongodb
mongo_client=pymongo.MongoClient("mongodb://localhost:27017")
mongo_db=mongo_client["d7astudents"]
mongo_collection=mongo_db["girls"]

#connect to postgresql
pg_conn=psycopg2.connect(
    host="localhost",
    database="d7astudents",
    user="postgres",
    password="a1b2c3"
)
pg_cursor=pg_conn.cursor()

#fetch data from mongodb 
mongo_data=mongo_collection.find()

#insert into postgreSQl
for record in mongo_data:
    try:
        #extract fields
        name=record.get("name")
        vesid=record.get("vesid")

        #insert into postgresql
        pg_cursor.execute(
            "INSERT INTO girls (name, vesid) VALUES (%s, %s)",
            (name, vesid)
        )

    except Exception as e:
        print(f"Error in migrating record {record}:{e} ")
        pg_conn.rollback()
    else:
        pg_conn.commit()

#close connections
pg_cursor.close()
pg_conn.close()
mongo_client.close()