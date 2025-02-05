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







