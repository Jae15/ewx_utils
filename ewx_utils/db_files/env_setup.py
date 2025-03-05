import os
import platform
import argparse
from pathlib import Path
from dotenv import load_dotenv

def get_base_dir(custom_path=None):
    """
    Get base directory with flexibility for different setups
    Parameters:
        custom_path: Optional custom path provided by user
    """
    if custom_path:
        return Path(custom_path) / 'ewx_utils'  # Add extra ewx_utils level
    
    # Get user's home directory (works for both Windows and Linux)
    home = Path.home()
    
    # Define possible paths for both operating systems
    if platform.system() == 'Windows':
        possible_paths = [
            home / 'data_file' / 'ewx_utils' / 'ewx_utils',     # Original structure
            home / 'Documents' / 'ewx_utils' / 'ewx_utils',      # Common Windows location
            home / 'Projects' / 'ewx_utils' / 'ewx_utils',       # Alternative
            home / 'ewx_utils' / 'ewx_utils',                    # Direct in home
            Path('C:/Program Files/ewx_utils/ewx_utils'),        # System-wide
        ]
    else:  # Linux and macOS
        possible_paths = [
            home / 'data_file' / 'ewx_utils' / 'ewx_utils',     # Original structure
            home / 'projects' / 'ewx_utils' / 'ewx_utils',       # Common Unix location
            home / 'ewx_utils' / 'ewx_utils',                    # Direct in home
            Path('/opt/ewx_utils/ewx_utils'),                    # System-wide
        ]
    
    # Checking for existing installation
    for path in possible_paths:
        if path.parent.exists() and (path.parent / 'ewx_utils').exists():
            print(f"Project root found at: {path}")
            confirm = input("Use detected path? (y/n) [y]: ")
            if confirm.lower() != 'n':
                return path
    
    # If no existing path confirmed, ask user for installation path
    print(f"\nOperating System: {platform.system()}")
    print("No existing installation found.")
    print("Available installation options:")
    for i, path in enumerate(possible_paths, 1):
        print(f"{i}. {path}")
    print(f"{len(possible_paths) + 1}. Custom path")
    
    while True:
        try:
            choice = input("\nSelect installation path (number) [1]: ")
            if not choice.strip():  # Default to first option if empty
                print(f"Using default path: {possible_paths[0]}")
                return possible_paths[0]
            
            choice = int(choice)
            if choice <= len(possible_paths):
                selected_path = possible_paths[choice - 1]
                print(f"Selected path: {selected_path}")
                return selected_path
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
        except Exception as e:
            print(f"Error: {e}")
            print(f"Using default path: {possible_paths[0]}")
            return possible_paths[0]

def create_env_template(base_dir):
    """Creates .env.example file with platform-specific paths"""
    project_dir = base_dir / 'ewx_utils'
    template_path = base_dir / '.env.example'
    
    template_content = """# EWX Utils Environment Configuration
# Automatically generated for {os_name}

# Base project directory
EWX_BASE_PATH="{base_dir}"

# Database configuration file location
DATABASE_CONFIG_FILE="{project_dir}/database.ini"

# Log file directory
EWX_LOG_FILE="{project_dir}/logs"
"""
    
    # Create parent directory if it doesn't exist
    base_dir.parent.mkdir(parents=True, exist_ok=True)
    
    with open(template_path, 'w') as f:
        f.write(template_content.format(
            os_name=platform.system(),
            base_dir=base_dir,
            project_dir=project_dir
        ))
    
    print(f"Template created at: {template_path}")

def initialize_env(base_dir):
    """Initialize the environment with the correct directory structure"""
    print(f"Initializing environment in: {base_dir}")
    
    # Create nested project directory structure
    project_dir = base_dir / 'ewx_utils'
    env_path = base_dir / '.env'
    
    # Create necessary directories
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / 'logs').mkdir(parents=True, exist_ok=True)
    
    if not env_path.exists():
        with open(env_path, 'w') as f:
            f.write(f'EWX_BASE_PATH={base_dir}\n')
            f.write(f'DATABASE_CONFIG_FILE={project_dir}/database.ini\n')
            f.write(f'EWX_LOG_FILE={project_dir}/logs\n')
        print(f".env file created at {env_path}")
    
    load_dotenv(env_path)
    
    # Print configuration
    print(f"EWX_BASE_PATH={base_dir}")
    print(f"DATABASE_CONFIG_FILE={project_dir}/database.ini")
    print(f"EWX_LOG_FILE={project_dir}/logs")

def main():
    parser = argparse.ArgumentParser(description='Setup EWX Utils environment')
    parser.add_argument('--path', help='Custom installation path')
    args = parser.parse_args()

    # Get base directory
    base_dir = get_base_dir(args.path)
    
    # Create template and initialize environment
    create_env_template(base_dir)
    initialize_env(base_dir)

if __name__ == "__main__":
    main()