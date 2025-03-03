# config/env_setup.py
from pathlib import Path
from dotenv import load_dotenv
import os
import platform
import argparse
import sys

def get_project_root():
    """
    Automatically detect the project root directory
    Returns absolute path to project root
    """
    try:
        # Getting the absolute path of current script
        current_file = Path(__file__).resolve()  # Get this file's location
        project_root = current_file.parent.parent  # Go up two levels in the directory
        
        # Verifying this is indeed the project root by checking for key files/directories
        if (project_root / 'ewx_utils').exists():
            print(f"Project root found at: {project_root}")
            return project_root
            
        # Using current working directory
        cwd = Path.cwd()
        if 'ewx_utils' in str(cwd):
            # Find the ewx_utils root directory
            while cwd.name != 'ewx_utils' and cwd.parent != cwd:
                cwd = cwd.parent
            if cwd.name == 'ewx_utils':
                print(f"Project root found at: {cwd}")
                return cwd
        
        # Using sys.path
        for path in sys.path:
            potential_root = Path(path)
            if (potential_root / 'ewx_utils').exists():
                print(f"Project root found at: {potential_root}")
                return potential_root
                
        raise FileNotFoundError("Could not automatically detect project root")
        
    except Exception as e:
        print(f"Warning: Automatic detection failed: {e}")
        return None

def get_base_dir(custom_path=None):
    """
    Get base directory with automatic detection and fallback options
    """
    # Trying automatic detection first
    auto_detected_path = get_project_root()
    
    if custom_path:
        path = Path(custom_path)
        print(f"Using custom path: {path}")
        return path
    
    if auto_detected_path:
        confirm = input(f"Use detected path: {auto_detected_path}? (y/n) [y]: ")
        if confirm.lower() != 'n':
            return auto_detected_path
    
    # Possible options for paths
    home = Path.home()
    
    if platform.system() == 'Windows':
        possible_paths = [
            auto_detected_path,                    # Automatically detected path
            home / 'data_file' / 'ewx_utils',     # Original structure
            home / 'Documents' / 'ewx_utils',      # Common Windows location
            Path.cwd(),                           # Current working directory
            Path('C:/Program Files/ewx_utils'),    # System-wide
        ]
    else:  # Linux
        possible_paths = [
            auto_detected_path,                    # Automatically detected path
            home / 'data_file' / 'ewx_utils',     # Original structure
            Path.cwd(),                           # Current working directory
            Path('/opt/ewx_utils'),               # System-wide
        ]
    
    # Removing None values and duplicates
    possible_paths = [p for p in possible_paths if p is not None]
    # Removing duplicates
    possible_paths = list(dict.fromkeys(possible_paths))  
    
    # Checking for existing installation
    for path in possible_paths:
        if path.exists() and (path / 'ewx_utils').exists():
            print(f"Found existing installation at: {path}")
            confirm = input("Use this path? (y/n) [y]: ")
            if confirm.lower() != 'n':
                return path
    
    # Alternatives of possible paths  
    print("\nSelect installation path:")
    for i, path in enumerate(possible_paths, 1):
        print(f"{i}. {path}")
    print(f"{len(possible_paths) + 1}. Custom path")
    
    while True:
        try:
            choice = input("\nSelect path (number) [1]: ").strip()
            if not choice:  # Default to first option
                return possible_paths[0]
            
            choice = int(choice)
            if choice <= len(possible_paths):
                return possible_paths[choice - 1]
            elif choice == len(possible_paths) + 1:
                while True:
                    custom = input("Enter custom path: ").strip()
                    path = Path(custom)
                    confirm = input(f"Confirm path: {path} (y/n): ")
                    if confirm.lower() == 'y':
                        return path
        except ValueError:
            print("Please enter a valid number")
        except Exception as e:
            print(f"Error: {e}")
            return possible_paths[0]

def create_env_template(base_dir):
    """Creates .env.example file with detected paths"""
    template_path = base_dir / '.env.example'
    
    template_content = """# EWX Utils Environment Configuration
# Automatically generated for {os_name}
# Project root: {base_dir}

# Base project directory (auto-detected)
EWX_BASE_PATH="{base_dir}"

# Database configuration file location
DATABASE_CONFIG_FILE="{base_dir}/ewx_utils/database.ini"

# Log file directory
EWX_LOG_FILE="{base_dir}/ewx_utils/logs"
"""
    
    with open(template_path, 'w') as f:
        f.write(template_content.format(
            os_name=platform.system(),
            base_dir=str(base_dir)
        ))
    
    print(f"Template created at: {template_path}")

def initialize_env(base_dir):
    """Initialize environment with detected paths"""
    print(f"Initializing environment in: {base_dir}")
    
    project_dir = base_dir / 'ewx_utils'
    env_path = base_dir / '.env'
    
    # Creating necessary directories
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / 'logs').mkdir(parents=True, exist_ok=True)
    
    if not env_path.exists():
        with open(env_path, 'w') as f:
            f.write(f'EWX_BASE_PATH={base_dir}\n')
            f.write(f'DATABASE_CONFIG_FILE={project_dir}/database.ini\n')
            f.write(f'EWX_LOG_FILE={project_dir}/logs\n')
        print(f".env file created at {env_path}")
    
    load_dotenv(env_path)
    
    # Verifying paths
    print(f"\nEnvironment Configuration:")
    print(f"Operating System: {platform.system()}")
    print(f"Base Path: {os.getenv('EWX_BASE_PATH')}")
    print(f"Database Config: {os.getenv('DATABASE_CONFIG_FILE')}")
    print(f"Log File Path: {os.getenv('EWX_LOG_FILE')}")

def main():
    parser = argparse.ArgumentParser(description='Setup EWX Utils environment')
    parser.add_argument('--path', help='Custom installation path')
    args = parser.parse_args()

    base_dir = get_base_dir(args.path)
    create_env_template(base_dir)
    initialize_env(base_dir)

if __name__ == "__main__":
    main()