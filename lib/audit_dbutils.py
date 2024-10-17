import sqlite3
import configparser


def connect_to_audit_db(create_schema=False):
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Extract audit database connection details
    db_path = config["database"]["audit_database"]

    # Connect to the audit database
    db_conn = sqlite3.connect(db_path)
    cursor = db_conn.cursor()

    # Create schema if requested
    if create_schema:
        create_audit_runs_table(cursor)
        create_audit_results_table(cursor)

    return db_conn, cursor


def create_audit_runs_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS AuditRuns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plugin_slug TEXT NOT NULL,
            plugin_version TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(plugin_slug, plugin_version, rule_id)
        )
        """
    )


def create_audit_results_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS AuditResults (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            path TEXT,
            start_line INTEGER,
            end_line INTEGER,
            severity TEXT,
            message TEXT,
            FOREIGN KEY(run_id) REFERENCES AuditRuns(id)
        )
        """
    )


def insert_audit_run(cursor, plugin_slug, plugin_version, rule_id):
    sql = """
    INSERT OR IGNORE INTO AuditRuns (plugin_slug, plugin_version, rule_id)
    VALUES (?, ?, ?)
    """
    cursor.execute(sql, (plugin_slug, plugin_version, rule_id))
    cursor.execute("SELECT id FROM AuditRuns WHERE plugin_slug = ? AND plugin_version = ? AND rule_id = ?", (plugin_slug, plugin_version, rule_id))
    return cursor.fetchone()[0]


def insert_audit_result(cursor, run_id, result):
    sql = """
    INSERT INTO AuditResults (
        run_id, path, start_line, end_line, severity, message
    ) VALUES (?, ?, ?, ?, ?, ?)
    """
    cursor.execute(sql, (run_id, *result))
