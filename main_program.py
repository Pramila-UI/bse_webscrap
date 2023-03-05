import schedule
import time
from bulkdeal_data import extract_bulk_deals_information , insert_data_into_db
import logs.logger_config

import logging
logger = logging.getLogger(__name__)

def exceute_job():
    try:
        start_time = time.time()
        res = extract_bulk_deals_information()

        """once the dataframe is generated and Insert the data into the mysql table"""
        if res['Status'] == "Success":
            db_res = insert_data_into_db(res['Result'])
            logger.info(db_res)
        else:
            logger.error(db_res)

        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"Total Time taken for execution : {total_time}")
        
    except Exception as e:
        logger.error(f"Exception while executing the job -- {e}")



schedule.every(1).minutes.do(exceute_job)

### Runing the job at every day at 09:00 am 
# schedule.every().day.at("09:00:00").do(exceute_job)


while True:
    schedule.run_pending()
    time.sleep(1)