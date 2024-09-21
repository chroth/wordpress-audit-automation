import sqlite3
import configparser
from datetime import datetime
from dateutil.relativedelta import relativedelta


def connect_to_db(create_schema=False):
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Extract database connection details
    db_config = config["database"]

    # Connect to the database server (initially without specifying the database)
    db_conn = sqlite3.connect(
        db_config["database"]
    )
    cursor = db_conn.cursor()
    try:
        # If schema creation is requested, create the database and table if they don't exist
        if create_schema:
            create_plugin_data_table(cursor)
            create_plugin_results_table(cursor)

    except sqlite3.DatabaseError as e:
        raise SystemExit(
            "Database {} does not exist. Please run with the '--create-schema' flag to create the database.".format(
                db_config["database"]
            )
        )

    return db_conn, cursor


def delete_results_table(cursor):
    cursor.execute("DROP TABLE IF EXISTS PluginResults")
    create_plugin_results_table(cursor)


def create_plugin_data_table(cursor):
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS PluginData (
        slug VARCHAR(255) PRIMARY KEY,
        version VARCHAR(255),
        active_installs INT,
        downloaded INT,
        last_updated DATETIME,
        added_date DATE,
        download_link TEXT
    )
    """
    )


def create_plugin_results_table(cursor):
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS PluginResults (
        id INT AUTO_INCREMENT PRIMARY KEY,
        slug VARCHAR(255),
        file_path VARCHAR(255),
        check_id VARCHAR(255),
        start_line INT,
        end_line INT,
        vuln_lines TEXT,
        FOREIGN KEY (slug) REFERENCES PluginData(slug)
    )
    """
    )


def insert_plugin_into_db(cursor, plugin):
    # Prepare SQL upsert statement
    sql = """
    INSERT INTO PluginData (slug, version, active_installs, downloaded, last_updated, added_date, download_link)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(slug) DO UPDATE SET
        version = excluded.version,
        active_installs = excluded.active_installs,
        downloaded = excluded.downloaded,
        last_updated = excluded.last_updated,
        added_date = excluded.added_date,
        download_link = excluded.download_link
    """

    # Prepare data for database insertion
    last_updated = plugin.get("last_updated", None)
    added_date = plugin.get("added", None)

    # Convert date formats if available
    if last_updated:
        last_updated = datetime.strptime(last_updated, "%Y-%m-%d %I:%M%p %Z").strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    if added_date:
        added_date = datetime.strptime(added_date, "%Y-%m-%d").strftime("%Y-%m-%d")

    data = (
        plugin["slug"],
        plugin.get("version", "N/A"),
        int(plugin.get("active_installs", 0)),
        int(plugin.get("downloaded", 0)),
        last_updated,
        added_date,
        plugin.get("download_link", "N/A"),
    )

    try:
        cursor.execute(sql, data)
    except sqlite3.ProgrammingError as e:
        raise SystemExit(
            "Table does not exist. Please run with the '--create-schema' flag to create the table."
        )


def insert_result_into_db(cursor, slug, result):
    sql = (
        "INSERT INTO PluginResults (slug, file_path, check_id, start_line, end_line, vuln_lines)"
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    data = (
        slug,
        result["path"],
        result["check_id"],
        result["start"]["line"],
        result["end"]["line"],
        result["extra"]["lines"],
    )
    try:
        cursor.execute(sql, data)

    except sqlite3.ProgrammingError as e:
        raise SystemExit(
            "Table does not exist. Please run with the '--create-schema' flag to create the table."
        )

def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()

sqlite3.register_adapter(datetime, adapt_datetime_iso)

def select_plugins_for_download(cursor, active_installs=0):
    sql = (
        "SELECT slug, download_link FROM PluginData "
        "WHERE active_installs > ? AND last_updated >= ?"
    )
    two_years_ago = datetime.now() - relativedelta(years=2)
    data = (
        active_installs,
        two_years_ago,
    )
    try:
        cursor.execute(sql, data)
        return cursor

    except sqlite3.ProgrammingError as e:
        raise SystemExit(
            "Table does not exist. Please run with the '--create-schema' flag to create the table."
        )
