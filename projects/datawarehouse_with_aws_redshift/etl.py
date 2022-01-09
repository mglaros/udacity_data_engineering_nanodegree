import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from cluster_helpers import load_config


def load_staging_tables(cur, conn):
    """loads the staging tables from S3"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """performs the inserts into the fact and dimension tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """driver program that authenticates to the Redshift cluster
    and loads the staging data from S3 into the staging tables and 
    lastly uses these to insert into the fact and dimension tables"""
    config = load_config()
    
    host = config["HOST"]
    db_name = config["DB_NAME"]
    db_user = config["DB_USER"]
    db_password = config["DB_PASSWORD"]
    db_port = config["DB_PORT"]

    conn = psycopg2.connect(f"host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()