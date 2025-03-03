# config/env_setup.py
from pathlib import Path
from dotenv import load_dotenv
import os
import platform

def get_base_dir():
    if platform.system() == 'Windows':
        return Path('C:/Users/mwangija/data_file/ewx_utils')
    else:  # Linux
        return Path(os.path.expanduser('~')) / 'data_file' / 'ewx_utils'

def create_env_template():
    base_dir = get_base_dir()
    template_path = base_dir / '.env.example'
    
    template_content = """# EWX Utils Environment Configuration

# Current Configuration
EWX_BASE_PATH="{base_dir}"
DATABASE_CONFIG_FILE="{base_dir}/ewx_utils/database.ini"
EWX_LOG_FILE="{base_dir}/ewx_utils/logs"
"""
    
    with open(template_path, 'w') as f:
        f.write(template_content.format(base_dir=str(base_dir)))
    
    print(f"Template created at: {template_path}")

def initialize_env():
    base_dir = get_base_dir()
    print(f"Using base directory: {base_dir}")
    
    project_dir = base_dir / 'ewx_utils'
    env_path = base_dir / '.env'
    
    if not env_path.exists():
        with open(env_path, 'w') as f:
            f.write(f'EWX_BASE_PATH={base_dir}\n')
            f.write(f'DATABASE_CONFIG_FILE={project_dir}/database.ini\n')
            f.write(f'EWX_LOG_FILE={project_dir}/logs\n')
        print(f".env file created at {env_path}")
    
    # Create directory structure
    log_dir = project_dir / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    
    load_dotenv(env_path)
    
    # Verify paths
    print(f"\nOperating System: {platform.system()}")
    print("Environment Configuration:")
    print(f"Base Path: {os.getenv('EWX_BASE_PATH')}")
    print(f"Database Config: {os.getenv('DATABASE_CONFIG_FILE')}")
    print(f"Log File Path: {os.getenv('EWX_LOG_FILE')}")

if __name__ == "__main__":
    # First create the template
    create_env_template()
    # Then initialize the environment
    initialize_env()