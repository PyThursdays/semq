
class UnavailablePartitionFiles(Exception):

    def __init__(self, path: str):
        self.path = path
        super().__init__(f"Partition Files not found: {path}")


class RequestIdentifierNotFoundInRequestFile(Exception):

    def __init__(self, req_id, req_file):
        self.req_id = req_id
        self.req_file = req_file
        super().__init__(f"Request ID {req_id} not found in request file: {req_file}")
