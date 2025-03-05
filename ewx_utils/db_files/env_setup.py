#!/usr/bin/env python3
"""Setup script for EWX Utils environment configuration and directory structure."""

import os
import platform
import argparse
from pathlib import Path
from dotenv import load_dotenv

def get_base_dir(custom_path=None):
    """
    Get base directory for project installation.
    Parameters:
        custom_path (str, optional): User-provided installation path
    Returns:
        Path: Selected base directory
    """
    if custom_path:
        base = Path(custom_path)
        return base / 'ewx_utils' / 'ewx_utils'
    
    home = Path.home()
    
    if platform.system() == 'Windows':
        possible_paths = [
            home / 'data_file' / 'ewx_utils' / 'ewx_utils',
            home / 'Documents' / 'ewx_utils' / 'ewx_utils',
            home / 'ewx_utils' / 'ewx_utils',
            Path('C:/Program Files/ewx_utils/ewx_utils'),
        ]
    else:
        possible_paths = [
            home / 'data_file' / 'ewx_utils' / 'ewx_utils',
            home / 'ewx_utils' / 'ewx_utils',
            Path('/opt/ewx_utils/ewx_utils'),
        ]
    
    for path in possible_paths:
        if path.parent.exists():
            print(f"Project root found at: {path}")
            confirm = input("Use detected path? (y/n) [y]: ")
            if confirm.lower() != 'n':
                return path
    
    selected_path = possible_paths[0]
    print(f"Using default path: {selected_path}")
    return selected_path

def create_env_template(base_dir):
    """Create template .env file with default configurations."""
    template_path = base_dir / '.env.example'
    
    template_content = """# EWX Utils Environment Configuration
# Automatically generated for {os_name}

EWX_BASE_PATH="{base_dir}"
DATABASE_CONFIG_FILE="{base_dir}/database.ini"
EWX_LOG_FILE="{base_dir}/logs"
"""
    
    with open(template_path, 'w') as f:
        f.write(template_content.format(
            os_name=platform.system(),
            base_dir=base_dir
        ))
    
    print(f"Template created at: {template_path}")

def initialize_env(base_dir):
    """Initialize environment with directories and configuration files."""
    print(f"Initializing environment in: {base_dir}")
    
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / 'logs').mkdir(parents=True, exist_ok=True)
    
    env_path = base_dir / '.env'
    
    if not env_path.exists():
        with open(env_path, 'w') as f:
            f.write(f'EWX_BASE_PATH="{base_dir}"\n')
            f.write(f'DATABASE_CONFIG_FILE="{base_dir}/database.ini"\n')
            f.write(f'EWX_LOG_FILE="{base_dir}/logs"\n')
        print(f".env file created at {env_path}")
    
    load_dotenv(env_path)
    
    print(f'EWX_BASE_PATH="{base_dir}"')
    print(f'DATABASE_CONFIG_FILE="{base_dir}/database.ini"')
    print(f'EWX_LOG_FILE="{base_dir}/logs"')

def main():
    """Setup EWX Utils environment with optional custom path."""
    parser = argparse.ArgumentParser(description='Setup EWX Utils environment')
    parser.add_argument('--path', help='Custom installation path')
    args = parser.parse_args()

    base_dir = get_base_dir(args.path)
    create_env_template(base_dir)
    initialize_env(base_dir)

if __name__ == "__main__":
    main()