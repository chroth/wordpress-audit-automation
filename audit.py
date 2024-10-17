import os
import argparse
import configparser
from concurrent.futures import ThreadPoolExecutor
from lib.dbutils import connect_to_db, select_plugins_for_audit
from lib.audit_dbutils import connect_to_audit_db, insert_audit_run, insert_audit_result
from tqdm import tqdm
import subprocess
import json

# Read configuration
config = configparser.ConfigParser()
config.read("config.ini")
PLUGIN_ROOT_PATH = config["paths"]["plugin_path"]

def run_semgrep(plugin_slug, semgrep_rule):
    plugin_path = os.path.join(PLUGIN_ROOT_PATH, plugin_slug)

    # Build the Semgrep CLI command
    command = [
        "semgrep",
        "--config", semgrep_rule,
        "--json",
        "--no-git-ignore",
        plugin_path
    ]

    # Execute the Semgrep CLI command
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Semgrep CLI failed: {e.stderr}")

def store_results(db_conn, cursor, plugin_slug, plugin_version, rule_id, semgrep_results):
    run_id = insert_audit_run(cursor, plugin_slug, plugin_version, rule_id)

    for result in semgrep_results.get("results", []):
        audit_result = (
            result.get("path"),
            result.get("start", {}).get("line"),
            result.get("end", {}).get("line"),
            result.get("extra", {}).get("severity"),
            result.get("extra", {}).get("message"),
        )
        insert_audit_result(cursor, run_id, audit_result)

    db_conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Run Semgrep audits in parallel.")
    parser.add_argument("-i", "--active-installs", required=True, type=int, help="Minimum active installs")
    parser.add_argument("-p", "--semgrep-rule-path", required=True, help="Full path to Semgrep rule")
    args = parser.parse_args()

    semgrep_rule = args.semgrep_rule_path
    if not os.path.exists(semgrep_rule):
        raise FileNotFoundError(f"Semgrep rule not found: {semgrep_rule}")

    db_conn, cursor = connect_to_db()
    plugins = select_plugins_for_audit(cursor, active_installs=args.active_installs).fetchall()
    print("Found plugins:", len(plugins))

    audit_db_conn, audit_cursor = connect_to_audit_db(create_schema=True)

    with tqdm(total=len(plugins), desc="Auditing Plugins") as pbar:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for plugin in plugins:
                plugin_slug, plugin_version, _ = plugin
                future = executor.submit(run_semgrep, plugin_slug, semgrep_rule)
                futures.append((future, plugin_slug, plugin_version))

            for future, plugin_slug, plugin_version in futures:
                semgrep_results = future.result()
                store_results(audit_db_conn, audit_cursor, plugin_slug, plugin_version, os.path.basename(semgrep_rule), semgrep_results)
                pbar.update(1)

    cursor.close()
    db_conn.close()
    audit_cursor.close()
    audit_db_conn.close()

if __name__ == "__main__":
    main()
