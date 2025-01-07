import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='notebook_execution.log')

def run_notebook(notebook_path):
    try:
        # Read the notebook
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)
        
        # Prepare execution
        ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
        
        # Execute the notebook
        logging.info(f"Starting execution of {notebook_path }")
        start_time = datetime.now()
        
        ep.preprocess(nb, {'metadata': {'path': os.path.dirname(notebook_path)}})
        
        # Save executed notebook with timestamp
        output_path = notebook_path.replace('.ipynb', 
            f'_executed_{start_time.strftime("%Y%m%d_%H%M%S")}.ipynb')
        
        with open(output_path, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)
        
        logging.info(f"Notebook execution completed. Output saved to {output_path}")
    
    except Exception as e:
        logging.error(f"Error executing notebook: {str(e)}")
        # Optionally send an alert or notification here

# Path to your notebook
NOTEBOOK_PATH = './script.ipynb'

if __name__ == '__main__':
    run_notebook(NOTEBOOK_PATH)