def drop_tables(conn, cur, drop_table_queries):
    """
    Executing "DROP" all tables
    :param conn: DB connection
    :param cur: DB cursor
    :return: None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(conn, cur, create_table_queries):
    """
    Executing "CREATE" all tables
    :param conn: DB connection
    :param cur: DB cursor
    :return: None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def process_create_tables(conn, drop_queries, create_queries):
    """
    Drop if exists tables
    Establish connection with stepstone DB and get cursor to it
    Create tables if not exists
    Close all connection

    :return: None
    """
    cur = conn.cursor()
    drop_tables(conn, cur, drop_queries)
    create_tables(conn, cur, create_queries)

    conn.close()
