import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from cluster_helpers import load_config, initialize


def drop_tables(cur, conn):
    """drops staging, fact, and dimension tables"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """creates staging, fact, and dimension tables"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """driver program that creates and initializes the Redshift cluster
    and authenticates and drops (if they already exist) and creates the staging, fact, and dimension tables"""
    #create and initialize the Redshift cluster
    initialize()
    
    config = load_config()
    
    host = config["HOST"]
    db_name = config["DB_NAME"]
    db_user = config["DB_USER"]
    db_password = config["DB_PASSWORD"]
    db_port = config["DB_PORT"]

    conn = psycopg2.connect(f"host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}")
    cur = conn.cursor()
    
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()