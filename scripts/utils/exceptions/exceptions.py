class Error(Exception):
    pass


class UnSupportedDialect(Error):
    def __init__(self, error_message="This dialect is not supported"):
        self.error_message = error_message
        super().__init__(self.error_message)
