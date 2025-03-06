#!/usr/bin/env python3
"""Setup script for EWX Utils environment configuration."""

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
        return Path(custom_path) / 'ewx_utils'
    
    # Get user's home directory
    home = Path.home()
    data_file_path = home / 'data_file' / 'ewx_utils'
    
    # Check if data_file directory exists
    if data_file_path.exists():
        print(f"Project root found at: {data_file_path}")
        confirm = input("Use detected path? (y/n) [y]: ")
        if confirm.lower() != 'n':
            return data_file_path
    
    # If path doesn't exist or user declined, show options
    print("\nAvailable installation paths:")
    possible_paths = [
        data_file_path,
        home / 'ewx_utils',
        home / ('Documents' if platform.system() == 'Windows' else 'documents') / 'ewx_utils'
    ]
    
    for i, path in enumerate(possible_paths, 1):
        print(f"{i}. {path}")
    print(f"{len(possible_paths) + 1}. Custom path")
    
    while True:
        try:
            choice = input("\nSelect installation path (number) [1]: ").strip()
            if not choice:  # Default to first option
                return possible_paths[0]
            
            choice = int(choice)
            if 1 <= choice <= len(possible_paths):
                return possible_paths[choice - 1]
            elif choice == len(possible_paths) + 1:
                while True:
                    custom = input("Enter custom path: ").strip()
                    custom_path = Path(custom) / 'ewx_utils'
                    confirm = input(f"Confirm path: {custom_path} (y/n): ")
                    if confirm.lower() == 'y':
                        return custom_path
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number")

def initialize_env(base_dir, force=False):
    """Initialize environment with directories and configuration files."""
    project_dir = base_dir / 'ewx_utils'
    env_path = base_dir / '.env'
    
    # Create directory structure
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / 'logs').mkdir(parents=True, exist_ok=True)
    
    # Create or overwrite .env file
    with open(env_path, 'w') as f:
        f.write(f'EWX_BASE_PATH="{base_dir}"\n')
        f.write(f'DATABASE_CONFIG_FILE="{project_dir}/database.ini"\n')
        f.write(f'EWX_LOG_FILE="{project_dir}/logs"\n')
    print(f".env file created at {env_path}")
    
    load_dotenv(env_path)
    
    # Display configuration
    print(f"\nOperating System: {platform.system()}")
    print("Environment Configuration:")
    print(f'EWX_BASE_PATH="{base_dir}"')
    print(f'DATABASE_CONFIG_FILE="{project_dir}/database.ini"')
    print(f'EWX_LOG_FILE="{project_dir}/logs"')

def main():
    """Setup EWX Utils environment with optional custom path."""
    parser = argparse.ArgumentParser(
        description='Setup EWX Utils environment',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--path', help='Custom installation path')
    parser.add_argument('--force', action='store_true', 
                       help='Force setup without confirmation')
    args = parser.parse_args()

    if not args.force:
        # Initial prompt for .env file creation
        create_env = input("Would you like to create a .env file? (y/n): ")
        if create_env.lower() != 'y':
            print("Setup cancelled.")
            return

    # Get the base directory first to check for existing .env
    base_dir = get_base_dir(args.path)
    env_path = base_dir / '.env'

    # Check for existing .env file
    if env_path.exists() and not args.force:
        overwrite = input("An existing .env file was found. Would you like to overwrite it? (y/n): ")
        if overwrite.lower() != 'y':
            print("Setup cancelled.")
            return

    # Confirm directory setup
    if not args.force and base_dir.exists():
        confirm = input(f"\nDirectory exists: {base_dir}\nProceed with setup? (y/n) [n]: ")
        if confirm.lower() != 'y':
            print("Setup cancelled.")
            return

    print(f"\nInitializing environment in: {base_dir}")
    initialize_env(base_dir, args.force)

if __name__ == "__main__":
    main()