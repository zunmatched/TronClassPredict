import os, schedule,time, logging, traceback
import schedule
from controller import *

os.makedirs('log', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%y-%m-%d %H:%M:%S',
    handlers=[logging.FileHandler(encoding='utf-8',filename='log/TronClassPredict.log')]
)

schedule.clear()
schedule.every().sunday.at('01:00').do(ctrl_message_prepare)
schedule.every().sunday.at('10:00').do(ctrl_message_push)

while __name__ == '__main__':
    try:
        schedule.run_pending()
    except:
        logging.error(traceback.format_exc())
    finally:
        time.sleep(1)