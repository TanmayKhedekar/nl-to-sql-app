import sqlite3

conn = sqlite3.connect("mydb.sqlite")
c = conn.cursor()
c.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
c.execute("INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 30)")
conn.commit()
conn.close()
