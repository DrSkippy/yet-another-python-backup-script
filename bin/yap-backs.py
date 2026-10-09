#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
import tarfile
import subprocess
import argparse
import yaml
import logging
import os


def create_mysql_dumps(databases, dest_dir, host='localhost', port=3306, 
                       username=None, password=None, compress=True, dryrun=True):
    """
    Create mysqldump files for a list of databases.
    
    Parameters
    ----------
    databases : list of str
        Names of databases to dump
    dest_dir : Path or str
        Destination directory for dump files
    host : str, optional
        MySQL host (default: 'localhost')
    port : int, optional
        MySQL port (default: 3306)
    username : str
        MySQL username
    password : str
        MySQL password
    compress : bool, optional
        If True, gzip the dump files (default: True)
    
    Returns
    -------
    tuple of (list of Path, list of str)
        Paths to the created dump files, and names of databases that failed

    Examples
    --------
    >>> dbs = ['production_db', 'analytics_db', 'staging_db']
    >>> dumps, failed = create_mysql_dumps(
    ...     dbs,
    ...     '/backups/mysql',
    ...     username='backup_user',
    ...     password='secure_password'
    ... )
    """
    logger = logging.getLogger(__name__)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"MySQL dump destination directory: {dest_dir}")

    created_dumps = []
    failed_databases = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logger.info(f"Starting MySQL dumps for {len(databases)} database(s)")

    for db_name in databases:
        # Create filename with timestamp
        extension = '.sql.gz' if compress else '.sql'
        dump_file = dest_dir / f"{db_name}_{timestamp}{extension}"
        
        # Build mysqldump command
        cmd = [
            'mysqldump',
            f'--host={host}',
            f'--port={port}',
            f'--user={username}',
            '--single-transaction',  # Consistent snapshot for InnoDB
            '--quick',               # Don't buffer entire result in memory
            '--lock-tables=false',   # Don't lock tables
        ]
        
        # Add password if provided
        if password:
            cmd.append(f'--password={password}')
        
        # Add database name
        cmd.append(db_name)

        logger.debug(f"Dump file will be created at: {dump_file}")

        if dryrun:
            logger.info(f"[DRY RUN] Dumping database: {db_name}")
            # Create sanitized command for logging (hide password)
            cmd_for_log = [part if '--password' not in part else '--password=***' for part in cmd]
            logger.debug(f"[DRY RUN] Command: {' '.join(cmd_for_log)}")
            created_dumps.append(dump_file)
            logger.info(f"[DRY RUN] Would create: {dump_file}")
        else:
            try:
                logger.info(f"Dumping database: {db_name}")
                
                if compress:
                    logger.debug(f"Using gzip compression for {db_name}")
                    # Pipe mysqldump through gzip
                    with open(dump_file, 'wb') as f:
                        dump_proc = subprocess.Popen(
                            cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE
                        )
                        gzip_proc = subprocess.Popen(
                            ['gzip'],
                            stdin=dump_proc.stdout,
                            stdout=f,
                            stderr=subprocess.PIPE
                        )
                        dump_proc.stdout.close()

                        # Wait for both processes
                        gzip_stderr = gzip_proc.communicate()[1]
                        dump_returncode = dump_proc.wait()

                        if dump_returncode != 0:
                            _, dump_stderr = dump_proc.communicate()
                            raise subprocess.CalledProcessError(
                                dump_returncode, cmd, stderr=dump_stderr
                            )

                    file_size = dump_file.stat().st_size
                    logger.debug(f"Dump file size: {file_size} bytes ({file_size / 1024 / 1024:.2f} MB)")
                else:
                    logger.debug(f"Dumping {db_name} without compression")
                    # Write directly to file
                    with open(dump_file, 'w') as f:
                        result = subprocess.run(
                            cmd,
                            stdout=f,
                            stderr=subprocess.PIPE,
                            text=True,
                            check=True
                        )

                    file_size = dump_file.stat().st_size
                    logger.debug(f"Dump file size: {file_size} bytes ({file_size / 1024 / 1024:.2f} MB)")

                created_dumps.append(dump_file)
                logger.info(f"Successfully created dump: {dump_file}")

            except subprocess.CalledProcessError as e:
                logger.error(f"Error dumping {db_name}: {e.stderr}")
                failed_databases.append(db_name)
                # Remove partial dump file if it exists
                if dump_file.exists():
                    logger.debug(f"Removing partial dump file: {dump_file}")
                    dump_file.unlink()
            except Exception as e:
                logger.error(f"Unexpected error dumping {db_name}: {e}")
                failed_databases.append(db_name)
                if dump_file.exists():
                    logger.debug(f"Removing partial dump file: {dump_file}")
                    dump_file.unlink()

    return created_dumps, failed_databases


def create_postgresql_dumps(databases, dest_dir, host='localhost', port=5432,
                            username=None, password=None, compress=True,
                            dump_globals=False, dryrun=True):
    """
    Create pg_dump files for a list of PostgreSQL databases.

    Optionally also dumps cluster-wide globals (roles, role passwords,
    tablespaces) via ``pg_dumpall --globals-only``, which pg_dump omits.

    Parameters
    ----------
    databases : list of str
        Names of databases to dump
    dest_dir : Path or str
        Destination directory for dump files
    host : str, optional
        PostgreSQL host (default: 'localhost')
    port : int, optional
        PostgreSQL port (default: 5432)
    username : str
        PostgreSQL username
    password : str
        PostgreSQL password (passed via PGPASSWORD env var)
    compress : bool, optional
        If True, gzip the dump files (default: True)
    dump_globals : bool, optional
        If True, also dump cluster globals to ``_globals_<timestamp>.sql[.gz]``
        (default: False)
    dryrun : bool, optional
        If True, log actions without creating files (default: True)

    Returns
    -------
    tuple of (list of Path, list of str)
        Paths to the created dump files, and names of databases that failed
        (``_globals`` if the globals dump failed)
    """
    logger = logging.getLogger(__name__)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"PostgreSQL dump destination directory: {dest_dir}")

    conn_args = [f'--host={host}', f'--port={port}', f'--username={username}']
    jobs = [(db_name, ['pg_dump', *conn_args, db_name]) for db_name in databases]
    if dump_globals:
        jobs.append(('_globals', ['pg_dumpall', *conn_args, '--globals-only']))

    created_dumps = []
    failed_databases = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logger.info(f"Starting PostgreSQL dumps for {len(databases)} database(s)")

    # Build subprocess env with PGPASSWORD set
    env = os.environ.copy()
    if password:
        env['PGPASSWORD'] = password

    for db_name, cmd in jobs:
        extension = '.sql.gz' if compress else '.sql'
        dump_file = dest_dir / f"{db_name}_{timestamp}{extension}"

        logger.debug(f"Dump file will be created at: {dump_file}")

        if dryrun:
            logger.info(f"[DRY RUN] Dumping database: {db_name}")
            logger.debug(f"[DRY RUN] Command: {' '.join(cmd)}")
            created_dumps.append(dump_file)
            logger.info(f"[DRY RUN] Would create: {dump_file}")
        else:
            try:
                logger.info(f"Dumping database: {db_name}")

                if compress:
                    logger.debug(f"Using gzip compression for {db_name}")
                    with open(dump_file, 'wb') as f:
                        dump_proc = subprocess.Popen(
                            cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            env=env
                        )
                        gzip_proc = subprocess.Popen(
                            ['gzip'],
                            stdin=dump_proc.stdout,
                            stdout=f,
                            stderr=subprocess.PIPE
                        )
                        dump_proc.stdout.close()

                        gzip_proc.communicate()
                        dump_returncode = dump_proc.wait()

                        if dump_returncode != 0:
                            _, dump_stderr = dump_proc.communicate()
                            raise subprocess.CalledProcessError(
                                dump_returncode, cmd, stderr=dump_stderr
                            )

                    file_size = dump_file.stat().st_size
                    logger.debug(f"Dump file size: {file_size} bytes ({file_size / 1024 / 1024:.2f} MB)")
                else:
                    logger.debug(f"Dumping {db_name} without compression")
                    with open(dump_file, 'w') as f:
                        subprocess.run(
                            cmd,
                            stdout=f,
                            stderr=subprocess.PIPE,
                            text=True,
                            check=True,
                            env=env
                        )

                    file_size = dump_file.stat().st_size
                    logger.debug(f"Dump file size: {file_size} bytes ({file_size / 1024 / 1024:.2f} MB)")

                created_dumps.append(dump_file)
                logger.info(f"Successfully created dump: {dump_file}")

            except subprocess.CalledProcessError as e:
                logger.error(f"Error dumping {db_name}: {e.stderr}")
                failed_databases.append(db_name)
                if dump_file.exists():
                    logger.debug(f"Removing partial dump file: {dump_file}")
                    dump_file.unlink()
            except Exception as e:
                logger.error(f"Unexpected error dumping {db_name}: {e}")
                failed_databases.append(db_name)
                if dump_file.exists():
                    logger.debug(f"Removing partial dump file: {dump_file}")
                    dump_file.unlink()

    return created_dumps, failed_databases


def create_gzipped_tarballs(backup_list, dryrun=True):
    """
    Create gzipped tarballs from a list of backup specifications.
    
    Parameters
    ----------
    backup_list : list of tuple
        Each tuple contains (source_paths, backup_root_path, filename):
        - source_paths: Path or list of Paths to be archived
        - backup_root_path: Path where tarball will be saved
        - filename: str, name of the tarball (will append .tar.gz if not present)
    
    Returns
    -------
    tuple of (list of Path, list of str)
        Paths to the created tarball files, and filenames that failed

    Examples
    --------
    >>> backup_jobs = [
    ...     (Path('/home/user/documents'), Path('/backups'), 'documents_backup'),
    ...     ([Path('/home/user/photos'), Path('/home/user/videos')],
    ...      Path('/backups'), 'media_backup'),
    ... ]
    >>> created, failed = create_gzipped_tarballs(backup_jobs)
    """
    logger = logging.getLogger(__name__)
    created_tarballs = []
    failed_tarballs = []
    logger.info(f"Starting tarball creation for {len(backup_list)} backup job(s)")

    for source_paths, backup_root, filename in backup_list:
        # Ensure backup root exists
        backup_root = Path(backup_root)
        backup_root.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Tarball destination directory: {backup_root}")

        # Ensure filename has .tar.gz extension
        if not filename.endswith('.tar.gz'):
            filename += '.tar.gz'

        tarball_path = backup_root / filename
        logger.debug(f"Tarball path: {tarball_path}")

        # Convert single path to list for uniform handling
        if isinstance(source_paths, (str, Path)):
            source_paths = [Path(source_paths)]
        else:
            source_paths = [Path(p) for p in source_paths]

        logger.info(f"Creating tarball with {len(source_paths)} source path(s): {filename}")

        if dryrun:
            for source_path in source_paths:
                if source_path.exists():
                    logger.info(f"[DRY RUN] Would add {source_path} to archive as {source_path.name}")
                    logger.debug(f"[DRY RUN] Source path exists and is accessible")
                else:
                    logger.warning(f"[DRY RUN] Source path does not exist: {source_path}")
            logger.info(f"[DRY RUN] Would create tarball: {tarball_path}")
        else:
            # Create the gzipped tarball
            try:
                with tarfile.open(tarball_path, 'w:gz') as tar:
                    for source_path in source_paths:
                        if source_path.exists():
                            logger.info(f"Adding {source_path} to archive as {source_path.name}")
                            tar.add(source_path, arcname=source_path.name)
                            logger.debug(f"Successfully added {source_path}")
                        else:
                            logger.warning(f"Source path does not exist, skipping: {source_path}")

                file_size = tarball_path.stat().st_size
                logger.debug(f"Tarball size: {file_size} bytes ({file_size / 1024 / 1024:.2f} MB)")
                logger.info(f"Successfully created tarball: {tarball_path}")
            except Exception as e:
                logger.error(f"Error creating tarball {tarball_path}: {e}")
                failed_tarballs.append(filename)
                if tarball_path.exists():
                    logger.debug(f"Removing partial tarball: {tarball_path}")
                    tarball_path.unlink()
                continue

        created_tarballs.append(tarball_path)

    return created_tarballs, failed_tarballs


def parse_arguments():
    """
    Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Yet Another Python Backup Script - Backup MySQL/PostgreSQL databases and files'
    )

    parser.add_argument(
        '--config',
        '-c',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )

    parser.add_argument(
        '--execute',
        '-x',
        action='store_true',
        help='Actually perform the backup. Without this flag, the script '
             'runs as a dry run and creates no files.'
    )

    parser.add_argument(
        '--log-level',
        '-l',
        type=str,
        default=None,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Set logging level (default: value from config.yaml, or INFO)'
    )

    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        help='Log file path (default: value from config.yaml, or logs/yap-backs.log)'
    )

    return parser.parse_args()


def setup_logging(log_level='INFO', log_file=None):
    """
    Configure logging with specified level and optional file output.

    Parameters
    ----------
    log_level : str, optional
        Logging level (DEBUG, INFO, WARNING, ERROR) (default: INFO)
    log_file : str, optional
        Path to log file. If None, logs only to console (default: None)
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    # Clear any existing handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Logging to file: {log_file}")


def load_config(config_path):
    """
    Load configuration from YAML file.

    Parameters
    ----------
    config_path : str
        Path to the YAML configuration file

    Returns
    -------
    dict
        Configuration dictionary

    Raises
    ------
    FileNotFoundError
        If the configuration file doesn't exist
    yaml.YAMLError
        If the configuration file is malformed
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    if config is None:
        raise ValueError(f"Configuration file is empty: {config_path}")

    return config


if __name__ == '__main__':
    # Parse command-line arguments
    args = parse_arguments()
    dryrun = not args.execute

    # Peek at the config file's logging section before full validation, so
    # a config-declared log level/file take effect for the earliest log
    # lines too. CLI flags always win over config.yaml.
    default_log_file = str(Path(__file__).resolve().parent.parent / "logs" / "yap-backs.log")
    logging_defaults = {}
    try:
        with open(args.config, 'r') as f:
            _peek = yaml.safe_load(f) or {}
        logging_defaults = _peek.get('logging', {}) or {}
    except (FileNotFoundError, yaml.YAMLError):
        pass

    log_level = args.log_level or logging_defaults.get('level', 'INFO')
    log_file = args.log_file or logging_defaults.get('file', default_log_file)

    # Setup logging
    setup_logging(log_level=log_level, log_file=log_file)
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("Yet Another Python Backup Script (YAP-BackS)")
    logger.info("=" * 60)

    # Load configuration
    try:
        logger.info(f"Loading configuration from: {args.config}")
        config = load_config(args.config)
        logger.debug(f"Configuration loaded successfully")
    except (FileNotFoundError, ValueError, yaml.YAMLError) as e:
        logger.error(f"Error loading configuration: {e}")
        exit(1)

    # Setup time-based paths
    current_time = datetime.now()
    year = current_time.strftime("%Y")
    timestamp = current_time.strftime("%Y-%m-%d_%H%M")

    backup_root_path = Path(config['backup']['root_path']) / year
    logger.info(f"Backup root path: {backup_root_path}")
    logger.debug(f"Timestamp: {timestamp}")

    if dryrun:
        logger.warning("Running in DRY RUN mode - no actual backups will be created")
        logger.info("")

    #########################
    # MySQL Backups
    #########################

    logger.info("Starting MySQL database backups...")
    mysql_config = config['mysql']
    logger.debug(f"MySQL host: {mysql_config['host']}:{mysql_config.get('port', 3306)}")
    logger.debug(f"Databases to backup: {', '.join(mysql_config['databases'])}")

    mysql_password = mysql_config.get('password') or os.environ.get('YAP_MYSQL_PASSWORD')
    if not mysql_password:
        logger.error(
            "No MySQL password found. Set YAP_MYSQL_PASSWORD in .envrc "
            "or a 'password' key under mysql: in config.yaml."
        )
        exit(1)

    dumps, failed_mysql = create_mysql_dumps(
        mysql_config['databases'],
        dest_dir=backup_root_path / "mysql_backups",
        host=mysql_config['host'],
        port=mysql_config.get('port', 3306),
        username=mysql_config['username'],
        password=mysql_password,
        compress=mysql_config.get('compress', True),
        dryrun=dryrun
    )

    logger.info(f"Completed MySQL backups: {len(dumps)} dump file(s)")
    if failed_mysql:
        logger.error(f"FAILED MySQL database(s): {', '.join(failed_mysql)}")
    logger.info("")

    #########################
    # PostgreSQL Backups
    #########################

    pg_dumps = []
    failed_pg = []
    if 'postgresql' in config:
        logger.info("Starting PostgreSQL database backups...")
        pg_config = config['postgresql']
        logger.debug(f"PostgreSQL host: {pg_config['host']}:{pg_config.get('port', 5432)}")
        logger.debug(f"Databases to backup: {', '.join(pg_config['databases'])}")

        pg_password = pg_config.get('password') or os.environ.get('YAP_POSTGRES_PASSWORD')
        if not pg_password:
            logger.error(
                "No PostgreSQL password found. Set YAP_POSTGRES_PASSWORD in "
                ".envrc or a 'password' key under postgresql: in config.yaml."
            )
            exit(1)

        pg_dumps, failed_pg = create_postgresql_dumps(
            pg_config['databases'],
            dest_dir=backup_root_path / "postgresql_backups",
            host=pg_config['host'],
            port=pg_config.get('port', 5432),
            username=pg_config['username'],
            password=pg_password,
            compress=pg_config.get('compress', True),
            dump_globals=pg_config.get('dump_globals', True),
            dryrun=dryrun
        )

        logger.info(f"Completed PostgreSQL backups: {len(pg_dumps)} dump file(s)")
        if failed_pg:
            logger.error(f"FAILED PostgreSQL database(s): {', '.join(failed_pg)}")
        logger.info("")

    #########################
    # File/Directory Backups
    #########################

    logger.info("Starting file/directory backups...")
    source_path_str = config['file_backups']['sources']
    source_paths = [Path(x) for x in source_path_str]
    source_names = ["backup" + z.replace("/", "-") + f"_{timestamp}" for z in source_path_str]
    logger.debug(f"Number of sources to backup: {len(source_paths)}")

    backup_jobs = [(x, backup_root_path, y) for x, y in zip(source_paths, source_names)]

    created_files, failed_files = create_gzipped_tarballs(backup_jobs, dryrun)

    logger.info(f"Completed file backups: {len(created_files)} tarball(s)")
    if failed_files:
        logger.error(f"FAILED tarball(s): {', '.join(failed_files)}")
    logger.info("")

    # Final summary
    all_failures = failed_mysql + failed_pg + failed_files
    logger.info("=" * 60)
    if dryrun:
        logger.warning("Dry run completed - no actual backups were created")
        logger.info(f"Would have created {len(dumps)} MySQL dump(s), {len(pg_dumps)} PostgreSQL dump(s), and {len(created_files)} tarball(s)")
        logger.info("Run with --execute to actually write backups.")
        logger.info("=" * 60)
    elif all_failures:
        logger.error(f"Backup completed WITH {len(all_failures)} FAILURE(S): {', '.join(all_failures)}")
        logger.info(f"Total: {len(dumps)} MySQL dump(s), {len(pg_dumps)} PostgreSQL dump(s), and {len(created_files)} tarball(s) succeeded")
        logger.info("=" * 60)
        exit(1)
    else:
        logger.info("Backup completed successfully!")
        logger.info(f"Total: {len(dumps)} MySQL dump(s), {len(pg_dumps)} PostgreSQL dump(s), and {len(created_files)} tarball(s)")
        logger.info("=" * 60)
