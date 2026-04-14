import requests


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str = "", chat_ids: list[str] | None = None, enabled: bool = True):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.chat_ids = list(chat_ids or [])
        if chat_id and chat_id not in self.chat_ids:
            self.chat_ids.insert(0, chat_id)
        self.enabled = enabled
        self.session = requests.Session()

    def _targets(self):
        return [chat_id for chat_id in self.chat_ids if chat_id]

    def send(self, text: str):
        if not self.enabled:
            return False
        if not self.bot_token or not self._targets():
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        sent_any = False

        for chat_id in self._targets():
            payload = {
                "chat_id": chat_id,
                "text": text,
            }

            try:
                response = self.session.post(url, json=payload, timeout=10)
                response.raise_for_status()
                data = response.json()
                sent_any = bool(data.get("ok", False)) or sent_any
            except Exception:
                continue

        return sent_any

    def test_connection(self):
        if not self.enabled:
            return {"enabled": False, "ok": False, "reason": "telegram_disabled"}
        if not self.bot_token or not self._targets():
            return {"enabled": True, "ok": False, "reason": "missing_telegram_credentials"}

        try:
            response = self.session.get(
                f"https://api.telegram.org/bot{self.bot_token}/getMe",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            return {
                "enabled": True,
                "ok": bool(data.get("ok", False)),
                "reason": "ok" if data.get("ok") else "telegram_api_rejected",
            }
        except Exception as e:
            return {"enabled": True, "ok": False, "reason": str(e)}
