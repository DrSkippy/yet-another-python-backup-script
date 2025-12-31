# Yet Another Python Backup Script (YAP-BackS)

A flexible Python-based backup solution for MySQL databases and file systems. Create automated backups with configurable settings through a simple YAML configuration file.

## Features

- **MySQL Database Backups**: Automated mysqldump with optional compression
- **File/Directory Backups**: Create gzipped tarballs of specified paths
- **YAML Configuration**: Centralized configuration management
- **Dry Run Mode**: Test backup operations without creating actual files
- **Organized Storage**: Automatic organization by year
- **Command-line Interface**: Simple CLI with flexible options

## Requirements

- Python 3.7+
- Poetry (for dependency management)
- MySQL client tools (for database backups)
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
  password: your_secure_password
  compress: true
  databases:
    - database1
    - database2
    - database3

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
```

### Configuration Options

#### MySQL Section
- `host`: MySQL server hostname (default: localhost)
- `port`: MySQL server port (default: 3306)
- `username`: MySQL username for authentication
- `password`: MySQL password for authentication
- `compress`: Enable gzip compression for dump files (default: true)
- `databases`: List of database names to backup

#### Backup Section
- `root_path`: Root directory where backups will be stored
  - Backups are automatically organized into year subdirectories

#### File Backups Section
- `sources`: List of file or directory paths to backup
  - Each path will be archived into a separate gzipped tarball

## Usage

### Basic Commands

Run backup with default configuration:
```bash
poetry run python bin/yap-backs.py
```

Run in dry-run mode (preview without creating files):
```bash
poetry run python bin/yap-backs.py --dry-run
```

Use a custom configuration file:
```bash
poetry run python bin/yap-backs.py --config /path/to/custom-config.yaml
```

Combine options:
```bash
poetry run python bin/yap-backs.py --config prod-config.yaml --dry-run
```

### Command-line Arguments

- `--config`, `-c`: Path to configuration file (default: `config.yaml`)
- `--dry-run`, `-n`: Perform dry run without creating actual backups
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
    └── backup-home-user-documents_2025-01-31_1430.tar.gz
    └── backup-home-user-projects_2025-01-31_1430.tar.gz
    └── backup-etc-important-configs_2025-01-31_1430.tar.gz
```

## Examples

### Example 1: Daily Automated Backups

Create a cron job for daily backups at 2 AM:

```bash
0 2 * * * cd /path/to/yet-another-python-backup-script && /usr/local/bin/poetry run python bin/yap-backs.py
```

### Example 2: Multiple Configuration Files

Maintain separate configurations for different environments:

```bash
# Production backups
poetry run python bin/yap-backs.py --config config-prod.yaml

# Development backups
poetry run python bin/yap-backs.py --config config-dev.yaml
```

### Example 3: Testing Configuration

Always test new configurations with dry-run first:

```bash
poetry run python bin/yap-backs.py --config new-config.yaml --dry-run
```

## Security Considerations

1. **Protect Configuration Files**: Ensure `config.yaml` has restricted permissions since it contains database credentials:
   ```bash
   chmod 600 config.yaml
   ```

2. **Use Dedicated Backup User**: Create a MySQL user with minimal required permissions:
   ```sql
   CREATE USER 'backup_user'@'localhost' IDENTIFIED BY 'secure_password';
   GRANT SELECT, LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'backup_user'@'localhost';
   ```

3. **Secure Backup Storage**: Ensure backup destination has appropriate access controls

4. **Don't Commit Secrets**: Add `config.yaml` to `.gitignore` to avoid committing sensitive credentials

## Troubleshooting

### MySQL Connection Errors

If you encounter MySQL connection errors, verify:
- MySQL credentials are correct
- MySQL server is running and accessible
- User has appropriate permissions
- Host and port settings are correct

### Permission Errors

If you encounter permission errors:
- Ensure the script has read access to source directories
- Ensure the script has write access to backup destination
- Check that the MySQL user has necessary database privileges

### Missing Dependencies

If you get import errors:
```bash
poetry install
# or
pip install pyyaml
```


