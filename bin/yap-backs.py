#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
import tarfile
import subprocess
import argparse
import yaml


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
    list of Path
        Paths to the created dump files
    
    Examples
    --------
    >>> dbs = ['production_db', 'analytics_db', 'staging_db']
    >>> dumps = create_mysql_dumps(
    ...     dbs, 
    ...     '/backups/mysql',
    ...     username='backup_user',
    ...     password='secure_password'
    ... )
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    created_dumps = []
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
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
        if dryrun:
            print(f"DRYRUN::Dumping {db_name}...")
            print(f'DRYRUN::Command: {" ".join(cmd)}')
            created_dumps.append(dump_file)
            print(f"DRYRUN::Created: {dump_file}")
        else:
            try:
                print(f"Dumping {db_name}...")
                
                if compress:
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
                else:
                    # Write directly to file
                    with open(dump_file, 'w') as f:
                        result = subprocess.run(
                            cmd,
                            stdout=f,
                            stderr=subprocess.PIPE,
                            text=True,
                            check=True
                        )
            
                created_dumps.append(dump_file)
                print(f"Created: {dump_file}")
            
            except subprocess.CalledProcessError as e:
                print(f"Error dumping {db_name}: {e.stderr}")
                # Remove partial dump file if it exists
                if dump_file.exists():
                    dump_file.unlink()
            except Exception as e:
                print(f"Unexpected error dumping {db_name}: {e}")
                if dump_file.exists():
                    dump_file.unlink()
    
    return created_dumps


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
    list of Path
        Paths to the created tarball files
    
    Examples
    --------
    >>> backup_jobs = [
    ...     (Path('/home/user/documents'), Path('/backups'), 'documents_backup'),
    ...     ([Path('/home/user/photos'), Path('/home/user/videos')], 
    ...      Path('/backups'), 'media_backup'),
    ... ]
    >>> created = create_gzipped_tarballs(backup_jobs)
    """
    created_tarballs = []
    
    for source_paths, backup_root, filename in backup_list:
        # Ensure backup root exists
        backup_root = Path(backup_root)
        backup_root.mkdir(parents=True, exist_ok=True)
        
        # Ensure filename has .tar.gz extension
        if not filename.endswith('.tar.gz'):
            filename += '.tar.gz'
        
        tarball_path = backup_root / filename
        
        # Convert single path to list for uniform handling
        if isinstance(source_paths, (str, Path)):
            source_paths = [Path(source_paths)]
        else:
            source_paths = [Path(p) for p in source_paths]
        
        if dryrun:
            for source_path in source_paths:
                if source_path.exists():
                    # Use arcname to preserve directory structure
                    print(f"DRYRUN::Adding {source_path} to tar archive {source_path.name}")
                else:
                    print(f"DRYRUN::Warning: {source_path} does not exist, skipping")
        else:
            # Create the gzipped tarball
            with tarfile.open(tarball_path, 'w:gz') as tar:
                for source_path in source_paths:
                    if source_path.exists():
                        # Use arcname to preserve directory structure
                        print(f"Adding {source_path} to tar archive {source_path.name}")
                        tar.add(source_path, arcname=source_path.name)
                    else:
                        print(f"Warning: {source_path} does not exist, skipping")
        
        created_tarballs.append(tarball_path)
        if dryrun:
            prefix = "DRYRUN::"
        else:
            prefix = ""
        print(f"{prefix}Created: {tarball_path}")
    
    return created_tarballs


def parse_arguments():
    """
    Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Yet Another Python Backup Script - Backup MySQL databases and files'
    )

    parser.add_argument(
        '--config',
        '-c',
        type=str,
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )

    parser.add_argument(
        '--dry-run',
        '-n',
        action='store_true',
        help='Perform a dry run without creating actual backups'
    )

    return parser.parse_args()


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
    dryrun = args.dry_run
    dryrun_prefix = "DRYRUN::" if dryrun else ""

    # Load configuration
    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError, yaml.YAMLError) as e:
        print(f"Error loading configuration: {e}")
        exit(1)

    # Setup time-based paths
    current_time = datetime.now()
    year = current_time.strftime("%Y")
    timestamp = current_time.strftime("%Y-%m-%d_%H%M")

    backup_root_path = Path(config['backup']['root_path']) / year

    if dryrun:
        print(f"{dryrun_prefix}Running in DRY RUN mode - no actual backups will be created\n")

    #########################
    # MySQL Backups
    #########################

    mysql_config = config['mysql']

    dumps = create_mysql_dumps(
        mysql_config['databases'],
        dest_dir=backup_root_path / "mysql_backups",
        host=mysql_config['host'],
        port=mysql_config.get('port', 3306),
        username=mysql_config['username'],
        password=mysql_config['password'],
        compress=mysql_config.get('compress', True),
        dryrun=dryrun
    )

    print(f"{dryrun_prefix}Created {len(dumps)} MySQL dump files\n")

    #########################
    # File/Directory Backups
    #########################

    source_path_str = config['file_backups']['sources']
    source_paths = [Path(x) for x in source_path_str]
    source_names = ["backup" + z.replace("/", "-") + f"_{timestamp}" for z in source_path_str]

    backup_jobs = [(x, backup_root_path, y) for x, y in zip(source_paths, source_names)]

    created_files = create_gzipped_tarballs(backup_jobs, dryrun)

    print(f"{dryrun_prefix}Created {len(created_files)} gzip'd tarballs")

    if dryrun:
        print(f"\n{dryrun_prefix}Dry run completed - no actual backups were created")
    else:
        print(f"\nBackup completed successfully!")
