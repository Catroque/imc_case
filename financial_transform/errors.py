from logger  import log

class Error:

    def __init__(self, msg: str, code: int = 400):
        log.error(msg)
        self.code   = code
        self.errors = [msg]

    def Message(self):
        return "\n".join(self.errors) if len(self.errors) > 0 else None
    
    def Code(self):
        return self.code
    