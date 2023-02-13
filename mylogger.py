import logging
from logging.handlers import RotatingFileHandler
from os.path import join
import datetime


def format_time():
    t = datetime.datetime.now()
    s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    return s[:-3]


logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.WARNING)

# add a rotating handler
log_dir_path = r"C:\Work\Data Engineer\Logs"
handler = logging.handlers.TimedRotatingFileHandler(filename=join(log_dir_path, 'DETask#2.log'), when='midnight',  backupCount=0)
handler.suffix = "%Y%m%d"
logger.addHandler(handler)


def log_entry(log_message):
    try:
        logger.warning(format_time() + ' - ' + log_message)
    except Exception as e:
        print(e)