import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops any existing tables in the cluster    
    Parameters
        cur: cursor connection to Redshift
        conn: connection to Redshift
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


def create_tables(cur, conn):
    """
    Createa new staging and dimensional tables as per sql_queries.py
    Parameters
        cur: cursor connection to Redshift
        conn: connection to Redshift
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


def main():
    """
    Set up database tables on Reedshift
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()