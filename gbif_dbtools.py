import pymysql


def query_db(sql):
    """
    Read and write from mySQL database
    :param sql: String script to run
    :return: Cursor
    """
    host, user, password, database = get_keys('localhost-permissions.txt')
    with pymysql.connect(host=host, user=user, password=password, db=database) as db:
        cursor = db.cursor()
        try:
            cursor.execute(sql)
            db.commit()
            return cursor
        except pymysql.Error as e:
            print(e)


def get_keys(filename):
    """
    Reads auth details from file
    :param filename: String filename
    :return: List<String> of auth details
    """
    with open(filename, 'r') as f:
        keys = f.read().splitlines()
        return keys


def update_db(sql, row_data):
    """
    Batch write to mySQL database
    :param sql: String script
    :param row_data: List of parameters to be used with the query
    :return: Cursor
    """
    host, user, password, database = get_keys('localhost-permissions.txt')
    with pymysql.connect(host=host, user=user, password=password, database=database) as db:
        cursor = db.cursor()
        try:
            cursor.executemany(sql, row_data)
            db.commit()
            return cursor
        except pymysql.Error as e:
            print(e)
