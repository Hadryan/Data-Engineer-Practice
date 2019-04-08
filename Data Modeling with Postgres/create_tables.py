import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    except psycopg2.Error as e:
        print("Error: could not make connection to the studentdb database")
    
    conn.set_session(autocommit=True)
    
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print(e)
    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print(e)

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("could not make connection to the sparkifydb database")
        
    cur = conn.cursor()
    
    return cur, conn


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)



    conn.close()


if __name__ == "__main__":
#     conn.close()
    main()