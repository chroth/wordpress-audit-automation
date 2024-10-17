import argparse
import configparser
import os
import subprocess
from tqdm import tqdm
from lib.dbutils import (
    connect_to_db,
    select_plugins_for_download,
    update_has_downloaded,
)

config = configparser.ConfigParser()
config.read("config.ini")
PLUGIN_ROOT_PATH = os.path.abspath(config["paths"]["plugin_path"])

def download_plugins(db_conn, cursor, active_installs, verbose=False):
    # Ensure the directory for plugins exists
    os.makedirs(PLUGIN_ROOT_PATH, exist_ok=True)
    os.makedirs("/tmp/downloads", exist_ok=True)


    plugins = select_plugins_for_download(cursor, active_installs).fetchall()
    plugins_count = len(plugins)
    pbar = tqdm(total=plugins_count)
    
    for (slug, version, download_link) in plugins:
        pbar.set_description(slug)
        # Download and extract the plugin
        download_and_extract_plugin(slug, version, download_link, verbose)
        # Update db
        update_has_downloaded(cursor, slug, has_downloaded=True)
        db_conn.commit()

        pbar.update(1)

    pbar.close()
    

def download_and_extract_plugin(slug, version, download_link, verbose):
    # Download and extract the plugin
    plugin_path = os.path.join(PLUGIN_ROOT_PATH, slug)

    # Clear the directory if it exists (update)
    if os.path.exists(plugin_path):
        if verbose:
            print(f"Plugin folder already exists, deleting folder: {plugin_path}")
        # Remove the plugin folder except for the .git directory
        subprocess.run(
            ["find", plugin_path, "-mindepth", "1", "-maxdepth", "1", "!", "-name", ".git", "-exec", "rm", "-r", "{}", ";"],
            check=True
        )

    # Download and extract
    if verbose:
        print(f"Downloading and extracting plugin: {slug}")

    download_target = os.path.join("/tmp/downloads", f"{slug}.{version}.zip")
    subprocess.run(
        ["curl", "--silent", "-o", download_target, download_link],
        check=True
    )
    subprocess.run(
        ["unzip", "-qq", "-o", download_target, "-d", PLUGIN_ROOT_PATH],
        check=True
    )
    os.unlink(download_target)
    
    # Check if the plugin is already version-controlled
    git_dir = os.path.join(plugin_path, ".git")
    if not os.path.exists(git_dir):
        subprocess.run(["git", "init", "-q"], cwd=plugin_path, check=True)
    subprocess.run(["git", "add", "."], cwd=plugin_path, check=True)
    try:
        subprocess.run(["git", "commit", "--quiet", "-m", version], cwd=plugin_path, check=True)
    except subprocess.CalledProcessError:
        return


def main():
    parser = argparse.ArgumentParser(
        description="Downloads Wordpress plugins."
    )
    parser.add_argument(
        "--active-installs",
        type=int,
        default=1000,
        help="Minimum amount of active installs to download (default: 1000)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print detailed messages"
    )

    # Parse arguments
    args = parser.parse_args()

    # Setup db
    db_conn, cursor = connect_to_db(False)

    # Download
    download_plugins(
        db_conn, cursor, args.active_installs, verbose=args.verbose
    )

    cursor.close()
    db_conn.close()

if __name__ == "__main__":
    main()