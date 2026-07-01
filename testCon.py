import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    database="reinex",
    user="rei",
    password="rei123"
)

print("Connected!")
conn.close()