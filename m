  print("\n1. Downloading CSV files from server...")
    success = get_csv('myserver', '/data/reports/*.csv', './data/csv/')
    
    if not success:
        print("Failed to download files. Exiting.")
        sys.exit(1)


        
import subprocess
import os
import sys
from datetime import datetime

# Add the CSV copy function at the top
def get_csv(server, remote_path, local_dir="./data/"):
    """Ultra-simple CSV copy"""
    os.makedirs(local_dir, exist_ok=True)
    result = subprocess.run(f"scp {server}:{remote_path} {local_dir}", shell=True, capture_output=True)
    if result.returncode == 0:
        print(f"✓ Downloaded CSV files to {local_dir}")
        return True
    else:
        print(f"✗ Failed to download CSVs")
        return False
