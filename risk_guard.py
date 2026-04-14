from datetime import datetime


class RiskGuard:
    def __init__(
        self,
        daily_loss_limit_usdt: float = 0.0,
        max_consecutive_losses: int = 0,
        max_total_drawdown_pct: float = 0.0,
    ):
        self.daily_loss_limit_usdt = daily_loss_limit_usdt
        self.max_consecutive_losses = max_consecutive_losses
        self.max_total_drawdown_pct = max_total_drawdown_pct
        self.day_key = self._today_key()
        self.day_realized_pnl = 0.0
        self.consecutive_losses = 0
        self.start_balance = None
        self.pause_reason = None

    def _today_key(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def ensure_day_rollover(self):
        today = self._today_key()
        if today == self.day_key:
            return
        self.day_key = today
        self.day_realized_pnl = 0.0
        self.consecutive_losses = 0
        self.pause_reason = None

    def hydrate(self, data: dict):
        self.day_key = data.get("day_key", self._today_key())
        self.day_realized_pnl = float(data.get("day_realized_pnl", 0.0))
        self.consecutive_losses = int(data.get("consecutive_losses", 0))
        self.start_balance = data.get("start_balance")
        self.pause_reason = data.get("pause_reason")
        self.ensure_day_rollover()

    def snapshot(self) -> dict:
        return {
            "day_key": self.day_key,
            "day_realized_pnl": self.day_realized_pnl,
            "consecutive_losses": self.consecutive_losses,
            "start_balance": self.start_balance,
            "pause_reason": self.pause_reason,
        }

    def initialize_balance(self, balance: float):
        if self.start_balance is None:
            self.start_balance = balance

    def register_closed_trade(self, pnl: float, balance_after: float):
        self.ensure_day_rollover()
        self.initialize_balance(balance_after - pnl)
        self.day_realized_pnl += pnl
        if pnl < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
        self._refresh_pause_reason(balance_after)

    def _refresh_pause_reason(self, balance: float):
        reason = None
        if self.daily_loss_limit_usdt > 0 and self.day_realized_pnl <= -self.daily_loss_limit_usdt:
            reason = f"daily_loss_limit_reached({self.day_realized_pnl:.2f})"
        elif self.max_consecutive_losses > 0 and self.consecutive_losses >= self.max_consecutive_losses:
            reason = f"max_consecutive_losses_reached({self.consecutive_losses})"
        elif self.max_total_drawdown_pct > 0 and self.start_balance:
            drawdown_pct = (self.start_balance - balance) / self.start_balance
            if drawdown_pct >= self.max_total_drawdown_pct:
                reason = f"max_drawdown_reached({drawdown_pct:.3f})"
        self.pause_reason = reason

    def can_open_new_position(self, balance: float) -> tuple[bool, str | None]:
        self.ensure_day_rollover()
        self.initialize_balance(balance)
        self._refresh_pause_reason(balance)
        if self.pause_reason:
            return False, self.pause_reason
        return True, None
