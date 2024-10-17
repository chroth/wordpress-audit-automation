import requests
import argparse
from tqdm import tqdm
from lib.dbutils import (
    connect_to_db,
    insert_plugin_into_db,
    update_plugin_in_db,
    get_current_version_from_slug,
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
            current_version = get_current_version_from_slug(cursor, plugin.get('slug'))
            if current_version == None:
                insert_plugin_into_db(cursor, plugin)
            elif current_version != plugin.get('version'):
                update_plugin_in_db(cursor, plugin)

            if verbose:
                print(f"Inserted data for plugin {plugin['slug']}.")
        db_conn.commit()


def main():
    parser = argparse.ArgumentParser(
        description="Downloads Wordpress plugins metadata."
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print detailed messages"
    )

    # Parse arguments
    args = parser.parse_args()

    # Create schema
    db_conn, cursor = connect_to_db()

    # Write plugins to CSV, Database, and possibly download them
    write_plugins_to_db(
        db_conn, cursor, args.verbose
    )

    cursor.close()
    db_conn.close()

if __name__ == "__main__":
    main()