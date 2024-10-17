from lib.dbutils import (
    connect_to_db,
)


def main():
    # Create schema
    db_conn, cursor = connect_to_db(True)
    cursor.close()
    db_conn.close()

if __name__ == "__main__":
    main()
