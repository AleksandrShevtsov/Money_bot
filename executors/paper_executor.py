class PaperExecutor:
    def __init__(self):
        self.enabled = True

    def place_order(self, *args, **kwargs):
        return {"status": "paper_only", "details": kwargs}
