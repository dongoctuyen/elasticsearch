import psycopg2
import json
from datetime import date

try:
    conn = psycopg2.connect(host="localhost", port = 5432, database="my_db", user="my_user", password="123")
    print("Connected successfully!")
except:
    print("Failed to connect postgreSQL!")

# Tao doi tuong cursor
cur = conn.cursor()

file_name = "./data/records_3.json"

postgres_insert_query = """ INSERT INTO status(date, content, share, reaction, fb_id) VALUES (%s, %s, %s, %s, %s);"""

with open(file_name, encoding='utf-8-sig') as f:
    data = json.loads(f.read())
    for line in data:
        record_to_insert = (line['date'], line['content'], line['share'], line['reaction'], line['fb_id'])
        
        try:
            cur.execute(postgres_insert_query, record_to_insert)
            print("insert sucessfully")
        except:
            print("fail to insert")
            with open("list_insert_fail.txt", 'a') as f_w:
                f_w.write(line['fb_id']+'\n')

        conn.commit()

print("Insert Done!")

