import json
import logging
import os
import sys
import traceback

LOGGER_BASE_NAME   = 'data-log'
LOGGER_FILTER_NAME = 'data-filter'


class CustomFilter(logging.Filter):

    def set_execution_id(self, value: str):
        self.execution_id = value

    def filter(self, record):
        
        if hasattr(self, 'execution_id'):
            record.execution_id = self.execution_id

        if "metric" in record.__dict__:
            record.metric = record.__dict__['metric']

        return True


class JsonFormatter(logging.Formatter):
    
    def format(self, log):
        pid = os.getpid()

        dt = {
            "severity":  log.levelname,
            "message":   log.getMessage(),
            "timestamp": self.formatTime(log),
            "pid":       pid,
        }

        if log.exc_info:
            dt["traceback"] = traceback.format_exc()

        if hasattr(log, 'execution_id'):
            dt["execution_id"] = str(log.execution_id)

        if hasattr(log, 'metric'):
            dt["metric"] = str(log.metric)

        return json.dumps(dt)


filter = CustomFilter(LOGGER_FILTER_NAME)
formatter = JsonFormatter()

stream = logging.StreamHandler(stream=sys.stdout)
stream.setFormatter(formatter)

log = logging.getLogger(LOGGER_BASE_NAME)
log.propagate = False
log.setLevel(logging.INFO)
log.addHandler(stream)
log.addFilter(filter)
