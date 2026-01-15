import logging
import os

def get_logger(name):
    log_dir = "/opt/airflow/logs/custom"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pipeline.log")

 
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    
    if not logger.handlers:
    
        c_handler = logging.StreamHandler()
        c_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'))
        
        
        f_handler = logging.FileHandler(log_file)
        f_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'))

        logger.addHandler(c_handler)
        logger.addHandler(f_handler)

    return logger