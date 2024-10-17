import argparse
from datetime import datetime, timedelta
from lib.dbutils import connect_to_db
from lib.audit_dbutils import connect_to_audit_db


def get_total_plugins(cursor, active_installs, two_years_ago):
    cursor.execute(
        """
        SELECT COUNT(0) 
        FROM PluginData 
        WHERE active_installs >= ? AND last_updated >= ?
        """,
        (active_installs, two_years_ago),
    )
    return cursor.fetchone()[0]


def get_completed_plugins(audit_cursor, semgrep_name):
    audit_cursor.execute(
        """
        SELECT COUNT(DISTINCT plugin_slug) 
        FROM AuditResults 
        WHERE rule_id LIKE ?
        """,
        (f"{semgrep_name}%",),
    )
    return audit_cursor.fetchone()[0]


def get_audit_status(active_installs, semgrep_name):
    # Calculate the date two years ago
    two_years_ago = (datetime.now() - timedelta(days=2 * 365)).strftime("%Y-%m-%d")

    # Connect to the plugin database
    plugin_conn, plugin_cursor = connect_to_db()

    # Get the total number of plugins to audit
    total_plugins = get_total_plugins(plugin_cursor, active_installs, two_years_ago)

    # Connect to the audit database
    audit_conn, audit_cursor = connect_to_audit_db()

    # Get the number of completed audits for the given Semgrep rule
    completed_plugins = get_completed_plugins(audit_cursor, semgrep_name)

    # Close database connections
    plugin_cursor.close()
    plugin_conn.close()
    audit_cursor.close()
    audit_conn.close()

    return completed_plugins, total_plugins


def main():
    parser = argparse.ArgumentParser(description="Check the status of a Semgrep audit.")
    parser.add_argument(
        "-i",
        "--active-installs",
        type=int,
        required=True,
        help="Minimum number of active installs to include in the audit.",
    )
    parser.add_argument(
        "-r",
        "--semgrep-rule",
        type=str,
        required=True,
        help="Name of the Semgrep rule being audited.",
    )
    args = parser.parse_args()

    completed, total = get_audit_status(args.active_installs, args.semgrep_rule)
    print(f"{completed}/{total}")


if __name__ == "__main__":
    main()
