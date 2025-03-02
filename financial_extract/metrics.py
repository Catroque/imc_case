from datetime import datetime, timedelta

from logger  import log

class Metrics:

    __slots__ = ["tms_start", "tms_last"]

    def __init__(self):
        self.tms_start = datetime.now()
        self.tms_last  = datetime.now()

    def registry(self, label: str, message: str = None, incremental: bool = False):
        if not incremental:
            self.tms_last = datetime.now()
            td = self.tms_last - self.tms_start
        else:
            aux = datetime.now()
            td = aux - self.tms_last
            self.tms_last = aux

        if message is None:
            message = label

        extra = {
            "metric": label
        }
        log.info("{} in {} seconds".format(message, td.total_seconds()), extra=extra)

    def reset(self, incremental: bool = False):
        if not incremental:
            self.tms_start = datetime.now()
            self.tms_last  = self.tms_start
        else:
            self.tms_last = datetime.now()
