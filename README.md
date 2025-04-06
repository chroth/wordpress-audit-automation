# Wordpress Plugin Audit Automation

This project automates the process of downloading, auditing, and analyzing Wordpress plugins for vulnerabilities. It leverages Semgrep for static analysis and stores results in a database for further review.

## Overview

The project has been significantly rewritten to streamline the workflow for my own use case. It now includes:

- Automated plugin metadata retrieval and database updates.
- Plugin downloading and version tracking with Git.
- Static analysis using Semgrep with customizable rules.
- Flexible auditing workflows for pattern-based or rule-based scans.
- Comprehensive database schema for tracking plugin metadata and audit results.

## Features

1. **Plugin Metadata Management**:

   - Fetch plugin metadata from the Wordpress API.
   - Store and update metadata in a database.
   - Track plugin versions and download status.

2. **Plugin Downloading**:

   - Download and extract plugins based on metadata.
   - Maintain version history using Git for each plugin.

3. **Static Analysis**:

   - Use Semgrep for rule-based vulnerability detection.
   - Support for custom patterns and rules.

4. **Auditing Workflows**:

   - Audit plugins based on active installs and last update date.
   - Flexible options for rule-based or pattern-based scans.

5. **Database Integration**:
   - SQLite database for storing plugin metadata and audit results.
   - Schema creation and management included.

## Audit Results Storage

The results of Semgrep audits are now stored in a separate SQLite database (`wpplugins-audit.db`). Each finding includes details such as:

- Plugin slug
- Rule ID and name
- Severity
- File path and line numbers
- Message

To analyze the results, query the `AuditResults` table in the `wpplugins-audit.db` database.

## Audit Metadata Tracking

Each audit run is now tracked in the `AuditRuns` table in the `wpplugins-audit.db` database. This includes:

- `plugin_slug`: The slug of the audited plugin.
- `plugin_version`: The version of the plugin at the time of the audit.
- `rule_id`: The Semgrep rule used for the audit.
- `timestamp`: The date and time of the audit.

Audit results are stored in the `AuditResults` table and linked to their corresponding run via the `run_id`. This ensures that the same rule is not re-run on the same plugin version.

## Getting Started

### Prerequisites

- Python 3.8+ and pip
- SQLite3
- Semgrep installed and configured
- Bash shell (for audit scripts)
- At least 30GB of disk space

### Installation

1. Clone this repository:

   ```
   git clone https://github.com/chroth/wordpress-audit-automation
   cd wordpress-audit-automation
   ```

2. Install Python dependencies:

   ```
   pip install -r requirements.txt
   ```

3. Install Semgrep:

   ```
   pip install semgrep
   semgrep login  # Optional: Log in for PRO rules if you have a Semgrep account
   ```

4. Configure the application:
   ```
   cp config.ini.sample config.ini
   vim config.ini
   ```
   Update the database path.

### Usage

#### Plugin Metadata Management

Retrieve and store plugin metadata in the database:

```
python3 plugin-update.py
```

#### Plugin Downloading

Download plugins based on metadata:

```
python3 plugin-download.py --active-installs 1000
```

#### Static Analysis

Run Semgrep audits using predefined rules:

```
python3 audit.py --active-installs 1000 --semgrep-rule-path /path/to/semgrep_rule.yaml
```

### Database Schema

The database schema includes a `PluginData` table with the following fields:

- `slug`: Unique identifier for the plugin.
- `version`: Current version of the plugin.
- `active_installs`: Number of active installs.
- `downloaded`: Total downloads.
- `last_updated`: Last update date.
- `added_date`: Date the plugin was added to the database.
- `download_link`: URL for downloading the plugin.
- `has_downloaded`: Boolean indicating if the plugin has been downloaded.

### Example Workflow

1. Fetch plugin metadata:

   ```
   python3 plugin-update.py
   ```

2. Download plugins:

   ```
   python3 plugin-download.py --active-installs 100000
   ```

3. Run a Semgrep audit:

   ```
   python3 audit.py --active-installs 100000 --semgrep-rule-path /path/to/semgrep_rule.yaml
   ```

4. Analyze results in the database.

### Troubleshooting

- Verify database credentials in `config.ini`.
- Check for sufficient disk space before running the scripts.

### Next Steps

1. Review audit results in the database.
2. Customize Semgrep rules for specific vulnerabilities.
3. Report findings responsibly.

This project is actively maintained and open to contributions. Feel free to submit issues or pull requests!
