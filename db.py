import duckdb

con = duckdb.connect("wiki.db")

con.execute("""
CREATE TABLE IF NOT EXISTS wiki_events (event_time TIMESTAMP,wiki TEXT,title TEXT,user TEXT,
    bot BOOLEAN,change_size INTEGER,comment TEXT
)
""")
