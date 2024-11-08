import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve paths from environment variables
ewx_base_path = os.getenv("EWX_BASE_PATH")
ewx_database_configfile = os.getenv("DATABASE_CONFIG_FILE")
ewx_log_file = os.getenv("EWX_LOG_FILE")

# Append paths to sys.path
sys.path.append(ewx_base_path)
sys.path.append(ewx_database_configfile)
sys.path.append(ewx_log_file)


"""
# Print the results
print(f"EWX Base Path: {ewx_base_path}")
print(f"Database Config File: {ewx_database_configfile}")
print(f"EWX Log File: {ewx_log_file}")

# Check if the paths exist
paths_to_check = [
    ewx_base_path,
    ewx_log_file,
    ewx_database_configfile
]

for path in paths_to_check:
    print(f"Path exists: {os.path.exists(path)} - {path}")

"""





