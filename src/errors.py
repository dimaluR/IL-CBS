class APIPathNotFound(Exception):
    """"raised when provided API path is not found"""
    def __init__(self, message='{"Message":"Error: Series Level Catalog"}'):
        self.message=message
        super().__init__(self.message)