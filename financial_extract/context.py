from logger  import log

class Context:

    __slots__ = ["metrics_cli", "secrets_cli", "storage_cli", "database_cli"]

    def __init__(self):
        self.metrics_cli  = None
        self.secrets_cli  = None
        self.storage_cli  = None
        self.database_cli = None
        self.execution    = Execution()


class Execution:

    __slots__ = ["project_id", "job_name", "execution_id"]

    def __init__(self):
        self.id = None
        self.execution_id = None
        self.job_name = None    
        self.tickers = []
        self.interval = None
        self.overlap = None
        self.bucket_name = None
        self.start_time = None
        self.end_time = None
        self.files_created = None
        self.records_processed = None
        self.status = None
        self.created_at = None
        self.updated_at = None
