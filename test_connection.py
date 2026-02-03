import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="postgres",
    password="dost",
    database="kruiz-dev-sql"
)

cur = conn.cursor()
cur.execute("SELECT NOW();")
print(cur.fetchone())
conn.close()
