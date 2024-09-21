import requests
import asyncio
import argparse
import os
import json
import subprocess
import zipfile
import shutil
from datetime import datetime
from io import BytesIO
from tqdm import tqdm
from dbutils import (
    connect_to_db,
    delete_results_table,
    insert_result_into_db,
    insert_plugin_into_db,
    select_plugins_for_download,
)


# Let's only retrieve 10 plugins per page so people feel like the status bar is actually moving
def get_plugins(page=1, per_page=100):
    url = f"https://api.wordpress.org/plugins/info/1.2/?action=query_plugins&request[page]={page}&request[per_page]={per_page}"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve page {page}: {response.status_code}")
        return None


def write_plugins_to_db(db_conn, cursor, verbose=False):

    # Get the first page to find out the total number of pages
    data = get_plugins(page=1)

    if not data or "info" not in data:
        print("Failed to retrieve the plugin information.")
        return

    total_pages = data["info"]["pages"]

    # Iterate through the pages
    for page in tqdm(range(1, total_pages + 1), desc="Storing plugins metadata"):
        data = get_plugins(page=page)

        if not data or "plugins" not in data:
            break

        for plugin in data["plugins"]:
            insert_plugin_into_db(cursor, plugin)

            if verbose:
                print(f"Inserted data for plugin {plugin['slug']}.")
        db_conn.commit()


async def download_plugins_in_database(cursor, download_dir, active_installs, replace_downloads=False, verbose=False):
    # Ensure the directory for plugins exists
    os.makedirs(os.path.join(download_dir, "plugins"), exist_ok=True)

    plugins = select_plugins_for_download(cursor, active_installs).fetchall()
    ongoing = []
    plugins_count = len(plugins)
    print("Length: %s" % plugins_count)
    pbar = tqdm(total=plugins_count)
    for (slug, download_link) in plugins:
        # Download and extract the plugin
        ongoing.append(download_and_extract_plugin(slug, download_link, download_dir, replace_downloads, verbose))
        
        if len(ongoing) == 10:
            await asyncio.gather(*ongoing)
            ongoing = []
            pbar.update(10)

    if len(ongoing) > 0:
        await asyncio.gather(*ongoing)
        pbar.update(len(ongoing))
    pbar.close()
    

async def download_and_extract_plugin(slug, download_link, download_dir, replace_downloads, verbose):
    # Download and extract the plugin
    plugin_path = os.path.join(download_dir, "plugins", slug)

    # Clear the directory if it exists
    if os.path.exists(plugin_path):
        if not replace_downloads:
            if verbose:
                print(f"Plugin folder already exists, skipping plugin: {plugin_path}")
            return
        if verbose:
            print(f"Plugin folder already exists, deleting folder: {plugin_path}")
        shutil.rmtree(plugin_path)

    try:
        if verbose:
            print(f"Downloading and extracting plugin: {slug}")
        response = requests.get(download_link)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extractall(os.path.join(download_dir, "plugins"))
    except requests.RequestException as e:
        print(f"Failed to download {slug}: {e}")
    except zipfile.BadZipFile:
        print(f"Failed to unzip {slug}: Not a zip file or corrupt zip file")


def run_semgrep_and_store_results(db_conn, cursor, download_dir, config, verbose=False):

    plugins = os.listdir(os.path.join(download_dir, "plugins"))

    for plugin in tqdm(plugins, desc="Auditing plugins"):
        plugin_path = os.path.join(download_dir, "plugins", plugin)
        output_file = os.path.join(plugin_path, "semgrep_output.json")

        command = [
            "semgrep",
            "--config",
            "{}".format(config),
            "--json",
            "--no-git-ignore",
            "--output",
            output_file,
            "--quiet",  # Suppress non-essential output
            plugin_path,
        ]

        try:
            # Run the semgrep command
            subprocess.run(command, check=True)
            if verbose:
                print(f"Semgrep analysis completed for {plugin}.")

        except subprocess.CalledProcessError as e:
            print(f"Semgrep failed for {plugin}: {e}")
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON for {plugin}: {e}")
        except Exception as e:
            print(f"Unexpected error for {plugin}: {e}")

        # Read the output file and process results
        with open(output_file, "r") as file:
            data = json.load(file)
            for item in data["results"]:
                insert_result_into_db(cursor, plugin, item)
                db_conn.commit()


async def main():
    parser = argparse.ArgumentParser(
        description="Downloads or audits all Wordpress plugins."
    )
    parser.add_argument(
        "--active-installs",
        type=int,
        default=0,
        help="Minimum amount of active installs to download",
    )
    parser.add_argument(
        '--store-plugins',
        action="store_true",
        help="Fetch all plugins metadata and store in database",
    )
    parser.add_argument(
        "--replace-downloads",
        action="store_true",
        help="Replace already downloaded plugins",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download and extract plugins, if plugin directory already exists, it will delete it and redownload",
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default=".",
        help="The directory to save/audit downloaded plugins (default: current directory)",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Audits downloaded plugins sequentially",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="p/php",
        help="Semgrep config/rules to run - https://semgrep.dev/docs/running-rules#running-semgrep-registry-rules-locally (default: p/php)",
    )
    parser.add_argument(
        "--create-schema",
        action="store_true",
        help="Create the database and schema if this flag is set",
    )
    parser.add_argument(
        "--clear-results",
        action="store_true",
        help="Clear audit table and then run, useful if run as a cron job and we only care about the latest release",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print detailed messages"
    )

    # Parse arguments
    args = parser.parse_args()

    if not args.store_plugins and not args.download and not args.audit:
        print("Please set either the --store-plugins, --download or --audit option.\n")
        parser.print_help()

    else:
        # Create schema
        db_conn, cursor = connect_to_db(args.create_schema)
        if args.clear_results:
            delete_results_table(cursor)

        # Write plugins to CSV, Database, and possibly download them
        if args.store_plugins:
            write_plugins_to_db(
                db_conn, cursor, args.verbose
            )
        if args.download:
            await download_plugins_in_database(
                cursor, args.download_dir, args.active_installs, args.replace_downloads, args.verbose
            )
        if args.audit:
            run_semgrep_and_store_results(
                db_conn, cursor, args.download_dir, args.config, args.verbose
            )

        cursor.close()
        db_conn.close()

if __name__ == "__main__":
    asyncio.run(main())