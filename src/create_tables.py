import connect_db
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Create and connect to the stepstone db

    :return: The connection and cursor to stepstone database
    """
    # Connect to default DB
    conn, cur = connect_db.get_db_connection(db_config_filepath='db.cfg', section='postgresql_default')
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    db_name = connect_db.config_parser('db.cfg', 'stepstone')['dbname']

    # Create stepstone DB with UTF8 encoding
    cur.execute('DROP DATABASE IF EXISTS %s' % db_name)
    cur.execute("CREATE DATABASE %s WITH ENCODING 'utf8' TEMPLATE template0" % db_name)

    # Close connection to default DB
    conn.close()

    # Connect to stepstone DB
    conn, cur = connect_db.get_db_connection(db_config_filepath='db.cfg', section='stepstone')
    cur = conn.cursor()

    return conn, cur


def drop_tables(conn, cur):
    """
    Executing "DROP" all tables
    :param conn: DB connection
    :param cur: DB cursor
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(conn, cur):
    """
    Executing "CREATE" all tables
    :param conn: DB connection
    :param cur: DB cursor
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def process_create_tables():
    """
    Drop if exists tables
    Establish connection with stepstone DB and get cursor to it
    Create tables if not exists
    Close all connection

    :return: None
    """
    conn, cur = create_database()
    drop_tables(conn, cur)
    create_tables(conn, cur)

    conn.close()


if __name__ == "__main__":
    process_create_tables()
