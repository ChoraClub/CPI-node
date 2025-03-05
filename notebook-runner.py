import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import logging
from datetime import datetime
import os
import time
import schedule
from pathlib import Path
import pytz  # You'll need to install pytz to work with timezones

# Configure logging to store logs in IST
def timestamp_ist():
    # Get current time in UTC and convert it to IST
    utc_time = datetime.now(pytz.utc)
    ist_time = utc_time.astimezone(pytz.timezone('Asia/Kolkata'))  # Convert to IST
    return ist_time.strftime('%Y-%m-%d %H:%M:%S')

class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # Override to use IST instead of UTC
        return timestamp_ist()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='notebook_execution.log'
)

logger = logging.getLogger()
for handler in logger.handlers:
    handler.setFormatter(CustomFormatter('%(asctime)s - %(levelname)s - %(message)s'))

class NotebookExecutor:
    def __init__(self, notebook_path):
        self.notebook_path = Path(notebook_path)
        self.is_running = False
        
    def run_notebook(self):
        # Prevent multiple concurrent executions
        if self.is_running:
            logging.warning("Previous execution still in progress, skipping this run")
            return
            
        self.is_running = True
        try:
            # Read the notebook
            with open(self.notebook_path, 'r', encoding='utf-8') as f:
                nb = nbformat.read(f, as_version=4)
            
            # Prepare execution
            ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
            
            # Execute the notebook
            logging.info(f"Starting execution of {self.notebook_path}")
            start_time = datetime.now()
            
            ep.preprocess(nb, {'metadata': {'path': str(self.notebook_path.parent)}})
            
            # Calculate execution time
            execution_time = datetime.now() - start_time
            
            # Overwrite the existing notebook
            with open(self.notebook_path, 'w', encoding='utf-8') as f:
                nbformat.write(nb, f)
                
            logging.info(f"Notebook execution completed in {execution_time}")
            
        except Exception as e:
            logging.error(f"Error executing notebook: {str(e)}")
        finally:
            self.is_running = False

def main():
    # Path to your notebook
    NOTEBOOK_PATH = './script.ipynb'
    
    # Create executor instance
    executor = NotebookExecutor(NOTEBOOK_PATH)
    
    # Immediately run the notebook
    logging.info("Initial execution started.")
    executor.run_notebook()
    
    # Schedule the job to run every 5 minutes
    schedule.every(5).minutes.do(executor.run_notebook)
    
    logging.info("Scheduler started. Notebook will execute every 5 minutes.")
    
    # Keep the script running
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Scheduler stopped by user")
            break
        except Exception as e:
            logging.error(f"Scheduler error: {str(e)}")
            # Wait a bit before retrying
            time.sleep(60)

if __name__ == '__main__':
    main()
