# load data from S3 into staging tables on Redshift 
# process that data into your analytics tables on Redshift

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Tables defines in the sql_queries.py loaded into Amazon Redshift database.
    
    Args:
    -------------------------------------
        cur:  database cursor
        conn: database connection
        
    return: 
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            
        except psycopg2.Error as e:
            print(e)


def insert_tables(cur, conn):
    """
    Tables defined in the sql_queries.py are inserted from the staging tables
    into the existing star schema tables in the redshift database.
    
    Args:
    -------------------------------------
        cur:  database cursor
        conn: database connection
        
    return: 
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            
        except psycopg2.Error as e:
            print(e)


def main():
    """
    Main function connects to the redshift database/cluster
    load staging tables
    insert existing tables from the staging tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} \
                            port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()