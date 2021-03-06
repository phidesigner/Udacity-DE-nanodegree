import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_number_rows_queries


def load_staging_tables(cur, conn):
    """
    Loads staging tables as created by sql_queries/copy_table_queries
        cur: cursor connection to Redshift
        conn: connection to Redshift
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts tables data as defined by sql_queries/insert_table_queries
        cur: cursor connection to Redshift
        conn: connection to Redshift
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        
def testing(cur, conn):
    """
    Checks if the data has been uploaded correctly as defined by sql_queries/select_number_rows_queries
        cur: cursor connection to Redshift
        conn: connection to Redshift
    """
    for query in select_number_rows_queries:
        print(query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print(row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    testing(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
