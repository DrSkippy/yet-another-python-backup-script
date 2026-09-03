# Yet Another Python Backup Script (YAP-BackS)

A flexible Python-based backup solution for MySQL databases, PostgreSQL databases, and file systems. Create automated backups with configurable settings through a simple YAML configuration file.

## Features

- **MySQL Database Backups**: Automated mysqldump with optional compression
- **PostgreSQL Database Backups**: Automated pg_dump with optional compression
- **File/Directory Backups**: Create gzipped tarballs of specified paths
- **YAML Configuration**: Centralized configuration management
- **Dry Run by Default**: Preview mode is the default; pass `--execute` to actually write backups
- **Organized Storage**: Automatic organization by year
- **Command-line Interface**: Simple CLI with flexible options

## Requirements

- Python 3.7+
- Poetry (for dependency management)
- MySQL client tools (`mysqldump`, for MySQL backups)
- PostgreSQL client tools (`pg_dump`, for PostgreSQL backups)
- gzip (for compression)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd yet-another-python-backup-script
```

2. Install dependencies using Poetry:
```bash
poetry install
```

Alternatively, install dependencies manually:
```bash
pip install pyyaml
```

## Configuration

Create or edit `config.yaml` in the project root with your backup settings:

```yaml
# MySQL Backup Configuration
mysql:
  host: localhost
  port: 3306
  username: backup_user
  # password is read from $YAP_MYSQL_PASSWORD (see .envrc.example) unless set here
  compress: true
  databases:
    - database1
    - database2
    - database3

# PostgreSQL Backup Configuration (optional section)
postgresql:
  host: localhost
  port: 5432
  username: backup_user
  # password is read from $YAP_POSTGRES_PASSWORD (see .envrc.example) unless set here
  compress: true
  databases:
    - database1

# Backup Root Path Configuration
backup:
  root_path: /path/to/backup/destination
  # Backups will be organized by year in subdirectories

# File/Directory Backup Configuration
file_backups:
  sources:
    - /home/user/documents
    - /home/user/projects
    - /etc/important-configs
    - /opt/application-data

# Logging Configuration (optional section)
logging:
  level: INFO
  file: logs/yap-backs.log
  # CLI --log-level/--log-file override these if given.
```

### Configuration Options

#### MySQL Section
- `host`: MySQL server hostname (default: localhost)
- `port`: MySQL server port (default: 3306)
- `username`: MySQL username for authentication
- `password`: MySQL password (optional — prefer `YAP_MYSQL_PASSWORD` in `.envrc`)
- `compress`: Enable gzip compression for dump files (default: true)
- `databases`: List of database names to backup

#### PostgreSQL Section (optional)
- `host`: PostgreSQL server hostname (default: localhost)
- `port`: PostgreSQL server port (default: 5432)
- `username`: PostgreSQL username for authentication
- `password`: PostgreSQL password (optional — prefer `YAP_POSTGRES_PASSWORD` in `.envrc`)
- `compress`: Enable gzip compression for dump files (default: true)
- `databases`: List of database names to backup
- Omit this whole section if you have no PostgreSQL databases to back up

#### Backup Section
- `root_path`: Root directory where backups will be stored
  - Backups are automatically organized into year subdirectories

#### File Backups Section
- `sources`: List of file or directory paths to backup
  - Each path will be archived into a separate gzipped tarball

#### Logging Section (optional)
- `level`: Logging level — `DEBUG`, `INFO`, `WARNING`, or `ERROR` (default: `INFO`)
- `file`: Log file path (default: `logs/yap-backs.log`)
- CLI flags `--log-level`/`--log-file` take priority over these if given

## Usage

### Basic Commands

Run in dry-run mode (default; preview without creating files):
```bash
poetry run python bin/yap-backs.py
```

Actually write backups:
```bash
poetry run python bin/yap-backs.py --execute
```

Use a custom configuration file:
```bash
poetry run python bin/yap-backs.py --config /path/to/custom-config.yaml --execute
```

Combine options:
```bash
poetry run python bin/yap-backs.py --config prod-config.yaml --execute
```

### Command-line Arguments

- `--config`, `-c`: Path to configuration file (default: `config.yaml`)
- `--execute`, `-x`: Actually perform the backup (default is a dry run; no files are written without this flag)
- `--log-level`, `-l`: Logging level, `DEBUG`/`INFO`/`WARNING`/`ERROR` (default: from `config.yaml`'s `logging.level`, else `INFO`)
- `--log-file`: Log file path (default: from `config.yaml`'s `logging.file`, else `logs/yap-backs.log`)
- `--help`, `-h`: Show help message and exit

## Output Structure

Backups are organized with the following structure:

```
/backup/root/path/
└── 2025/
    ├── mysql_backups/
    │   ├── database1_20250131_1430.sql.gz
    │   ├── database2_20250131_1430.sql.gz
    │   └── database3_20250131_1430.sql.gz
    ├── postgresql_backups/
    │   └── database1_20250131_1430.sql.gz
    └── backup-home-user-documents_2025-01-31_1430.tar.gz
    └── backup-home-user-projects_2025-01-31_1430.tar.gz
    └── backup-etc-important-configs_2025-01-31_1430.tar.gz
```

## Examples

### Example 1: Daily Automated Backups

Create a cron job for daily backups at 2 AM:

```bash
0 2 * * * cd /path/to/yet-another-python-backup-script && /usr/local/bin/poetry run python bin/yap-backs.py --execute
```

Note: `--execute` is required — without it the script only runs a dry-run preview and writes nothing.

### Example 2: Multiple Configuration Files

Maintain separate configurations for different environments:

```bash
# Production backups
poetry run python bin/yap-backs.py --config config-prod.yaml

# Development backups
poetry run python bin/yap-backs.py --config config-dev.yaml
```

### Example 3: Testing Configuration

Always test new configurations with dry-run first (the default with no flags):

```bash
poetry run python bin/yap-backs.py --config new-config.yaml
```

## Security Considerations

1. **Credentials via .envrc**: Database passwords are not stored in `config.yaml`. Set `YAP_MYSQL_PASSWORD` and `YAP_POSTGRES_PASSWORD` in `.envrc` (loaded via direnv; see `.envrc.example`). Ensure `.envrc` has restricted permissions:
   ```bash
   chmod 600 .envrc
   ```

2. **Use Dedicated Backup User (MySQL)**: Create a MySQL user with minimal required permissions:
   ```sql
   CREATE USER 'backup_user'@'localhost' IDENTIFIED BY 'secure_password';
   GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'backup_user'@'localhost';
   ```

3. **Use a Read-Only Role (PostgreSQL)**: Rather than a superuser, grant the backup user the built-in read-only role (PostgreSQL 14+) — this covers every current *and future* database on the cluster without per-database grants:
   ```sql
   GRANT pg_read_all_data TO backup_user;
   ```
   Avoid using the `postgres` superuser account for backups — it bypasses all permission checks (read, write, DDL) across the whole cluster, which is a much larger blast radius than a backup tool needs.

4. **Secure Backup Storage**: Ensure backup destination has appropriate access controls

5. **Don't Commit Secrets**: `config.yaml` and `.envrc` are already in `.gitignore`; keep it that way

## Troubleshooting

### MySQL Connection Errors

If you encounter MySQL connection errors, verify:
- MySQL credentials are correct
- MySQL server is running and accessible
- User has appropriate permissions
- Host and port settings are correct

### PostgreSQL Connection or Permission Errors

`pg_dump` fails with `permission denied for table ...` when the configured
user can connect but lacks `SELECT` on that database's tables — common when
a database was provisioned with its own app-specific owning role. Fix by
granting the backup user the read-only role (see Security Considerations
above): `GRANT pg_read_all_data TO backup_user;`. This is a one-time,
cluster-wide grant — no need to repeat it per database.

Also verify:
- PostgreSQL server is running and accessible on the configured host/port
- Credentials are correct (`YAP_POSTGRES_PASSWORD` in `.envrc`, or `password` in config.yaml)

### Permission Errors

If you encounter permission errors:
- Ensure the script has read access to source directories
- Ensure the script has write access to backup destination
- Check that the MySQL/PostgreSQL user has necessary database privileges
- The script exits non-zero on any dump/tarball failure — check the log
  file (default `logs/yap-backs.log`) for `ERROR` lines rather than
  assuming a completed run means everything succeeded

### Missing Dependencies

If you get import errors:
```bash
poetry install
# or
pip install pyyaml
```


