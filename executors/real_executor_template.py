class RealExecutorTemplate:
    def __init__(self, api_key="", secret_key=""):
        self.api_key = api_key
        self.secret_key = secret_key

    def place_order(self, symbol, side, qty):
        raise NotImplementedError("Подключи биржу и реализуй place_order")
