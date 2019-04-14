import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    try: 
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        print("Connected to default database")
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the Database")
        print(e)
    
    # --------------------------------------------------------------------------------------
    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        print("Dropped the sparkifydb database")
    except psycopg2.Error as e: 
        print("Error: Could not drop the Database")
        print(e)
    else:
        try:
            cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
            print("Created the sparkifydb database")
        except psycopg2.Error as e: 
            print("Error: Could not create the Database")
            print(e)

    # --------------------------------------------------------------------------------------
    # close connection to default database
    conn.close()

    
    # --------------------------------------------------------------------------------------
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        print("Connected to sparkifydb database")
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the sparkifydb database")
        print(e)
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e: 
            print("Error: Dropping table")
            print(query)
            print(e)


def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e: 
            print("Error: Issue creating table")
            print(query)
            print(e)


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()