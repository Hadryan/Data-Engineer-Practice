# create the fact and dimension tables for the star schema in Redshift

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Existed tables which are created by create_tables() functjiokn, are dropped from the database.
    
    Args:
    ------------------------------------
        cur:   database cursor  
        conn:  database connection
    
    Returns:
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


def create_tables(cur, conn):
    """
    Tables defined in the sql_queries.py file are created in the Redshift database.
    
    Args:
    -------------------------------------
        cur:   database cursor  
        conn:  database connection
    
    Returns:
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


def main():
    """
    Main funciton connects to the Redshift database, drop existing and creat new tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} \
                             port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()