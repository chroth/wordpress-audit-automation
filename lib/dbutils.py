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
        db_config["plugin_database"]
    )
    cursor = db_conn.cursor()
    try:
        # If schema creation is requested, create the database and table if they don't exist
        if create_schema:
            create_plugin_data_table(cursor)

    except sqlite3.DatabaseError as e:
        raise SystemExit(
            "Database {} does not exist. Please run with the '--create-schema' flag to create the database.".format(
                db_config["plugin_database"]
            )
        )

    return db_conn, cursor


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
        download_link TEXT,
        has_downloaded BOOLEAN
    )
    """
    )

def get_current_version_from_slug(cursor, slug):
    sql = "SELECT version FROM PluginData where slug=?"
    data = (slug,)
    cursor.execute(sql, data)
    version = next(cursor, [None])[0]
    return version

def update_has_downloaded(cursor, slug, has_downloaded=False):
    sql = "UPDATE PluginData SET has_downloaded=? WHERE slug=?"
    try:
        cursor.execute(sql, (has_downloaded, slug,))
        return cursor
    except sqlite3.ProgrammingError as e:
        raise SystemExit(
            "Table does not exist. Please run with the '--create-schema' flag to create the table."
        )

def update_plugin_in_db(cursor, plugin):
    # Prepare SQL update statement
    sql = "UPDATE PluginData SET version=?, active_installs=?, downloaded=?, last_updated=?, download_link=?, has_downloaded=false WHERE slug=?"

    # Prepare data for database insertion
    last_updated = plugin.get("last_updated", None)

    # Convert date formats if available
    if last_updated:
        last_updated = datetime.strptime(last_updated, "%Y-%m-%d %I:%M%p %Z").strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    data = (
        plugin.get("version", "N/A"),
        int(plugin.get("active_installs", 0)),
        int(plugin.get("downloaded", 0)),
        last_updated,
        plugin.get("download_link", "N/A"),
        plugin["slug"],
    )

    try:
        cursor.execute(sql, data)
        return cursor
    except sqlite3.ProgrammingError as e:
        raise SystemExit(
            "Table does not exist. Please run with the '--create-schema' flag to create the table."
        )

def insert_plugin_into_db(cursor, plugin):
    # Prepare SQL upsert statement
    sql = """
    INSERT INTO PluginData (slug, version, active_installs, downloaded, last_updated, added_date, download_link, has_downloaded)
    VALUES (?, ?, ?, ?, ?, ?, ?, false)
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


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()

sqlite3.register_adapter(datetime, adapt_datetime_iso)

def select_plugins_for_download(cursor, active_installs=0):
    sql = (
        "SELECT slug, version, download_link FROM PluginData "
        "WHERE active_installs >= ? AND last_updated >= ? AND has_downloaded = false"
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

def select_plugins_for_audit(cursor, active_installs=0):
    sql = (
        "SELECT slug, version, download_link FROM PluginData "
        "WHERE active_installs >= ? AND last_updated >= ?"
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
