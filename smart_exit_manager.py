class SmartExitManager:
    def __init__(self):
        self.default_profile = {
            "partial_tp_trigger": 0.50,
            "partial_close_fraction": 0.50,
            "trail_trigger": 0.85,
            "trail_gap_pct": 0.006,
            "atr_trail_mult": 1.6,
            "profit_be_partial_trigger": 0.50,
            "stop_lock_steps": (
                (0.30, 0.10),
                (0.50, 0.20),
            ),
        }
        self.class_profiles = {
            "BASE_A": {
                "partial_tp_trigger": 0.62,
                "partial_close_fraction": 0.35,
                "trail_trigger": 0.95,
                "profit_be_partial_trigger": 0.62,
                "stop_lock_steps": (
                    (0.35, 0.10),
                    (0.62, 0.25),
                ),
            },
            "PATTERN_A": {
                "partial_tp_trigger": 0.60,
                "partial_close_fraction": 0.35,
                "trail_trigger": 0.93,
                "profit_be_partial_trigger": 0.60,
                "stop_lock_steps": (
                    (0.35, 0.10),
                    (0.60, 0.22),
                ),
            },
            "OB_A": {
                "partial_tp_trigger": 0.55,
                "partial_close_fraction": 0.45,
                "trail_trigger": 0.88,
                "profit_be_partial_trigger": 0.55,
                "stop_lock_steps": (
                    (0.32, 0.10),
                    (0.55, 0.22),
                ),
            },
            "REVERSAL_A": {
                "partial_tp_trigger": 0.58,
                "partial_close_fraction": 0.40,
                "trail_trigger": 0.90,
                "profit_be_partial_trigger": 0.58,
                "stop_lock_steps": (
                    (0.35, 0.10),
                    (0.58, 0.22),
                ),
            },
            "REVERSAL_DIV": {
                "partial_tp_trigger": 0.55,
                "partial_close_fraction": 0.45,
                "trail_trigger": 0.88,
                "profit_be_partial_trigger": 0.55,
                "stop_lock_steps": (
                    (0.32, 0.10),
                    (0.55, 0.20),
                ),
            },
            "A": {
                "partial_tp_trigger": 0.55,
                "partial_close_fraction": 0.45,
                "trail_trigger": 0.88,
                "profit_be_partial_trigger": 0.55,
            },
            "B": {
                "partial_tp_trigger": 0.48,
                "partial_close_fraction": 0.50,
                "trail_trigger": 0.82,
                "profit_be_partial_trigger": 0.48,
            },
            "C": {
                "partial_tp_trigger": 0.30,
                "partial_close_fraction": 0.65,
                "trail_trigger": 0.62,
                "trail_gap_pct": 0.005,
                "profit_be_partial_trigger": 0.30,
                "stop_lock_steps": (
                    (0.20, 0.05),
                    (0.30, 0.12),
                ),
            },
            "REJECT": {
                "partial_tp_trigger": 0.24,
                "partial_close_fraction": 0.75,
                "trail_trigger": 0.52,
                "trail_gap_pct": 0.0045,
                "profit_be_partial_trigger": 0.24,
                "stop_lock_steps": (
                    (0.18, 0.03),
                    (0.24, 0.08),
                ),
            },
        }

        self.min_hold_seconds = 180
        self.strong_hold_seconds = 1200
        self.strong_signal_score = 0.85

        # Главная правка: не душим сделки слишком рано.
        # 15m стратегия не должна закрываться через 2–3 минуты.
        self.early_exit_enabled = False
        self.early_exit_check_seconds_c = 20 * 60     # только после ~1 завершенной 15m свечи
        self.early_exit_check_seconds_reject = 15 * 60
        self.early_exit_min_progress_c = 0.08
        self.early_exit_min_progress_reject = 0.05

    def _profile(self, pos):
        signal_class = pos.get("signal_class", "REJECT")
        profile = dict(self.default_profile)
        profile.update(self.class_profiles.get(signal_class, {}))
        return profile

    def progress_to_take(self, pos, price):
        entry = pos["entry"]
        take = pos["take"]

        if pos["side"] == "BUY":
            if take == entry:
                return 0.0
            return (price - entry) / (take - entry)

        if take == entry:
            return 0.0
        return (entry - price) / (entry - take)

    def unrealized_pnl(self, pos, price):
        if pos["side"] == "BUY":
            return (price - pos["entry"]) * pos["qty"]
        return (pos["entry"] - price) * pos["qty"]

    def pnl_pct_on_margin(self, pos, price):
        margin = pos.get("margin", 0.0)
        if margin <= 0:
            return 0.0
        return self.unrealized_pnl(pos, price) / margin

    def reward_progress(self, pos, price):
        entry = float(pos.get("entry", 0.0) or 0.0)
        take = float(pos.get("take", entry) or entry)
        qty = float(pos.get("qty", 0.0) or 0.0)
        target_reward = abs(take - entry) * qty
        if target_reward <= 0:
            return 0.0
        return max(0.0, self.unrealized_pnl(pos, price) / target_reward)

    def should_be_and_partial_on_profit(self, pos, price):
        if pos.get("partial_done"):
            return False
        return self.reward_progress(pos, price) >= self._profile(pos)["profit_be_partial_trigger"]

    def apply_break_even(self, pos):
        old_stop = pos["stop"]

        if pos["side"] == "BUY":
            pos["stop"] = max(old_stop, pos["entry"])
        else:
            pos["stop"] = min(old_stop, pos["entry"])

        pos["be_moved"] = True
        return old_stop, pos["stop"]

    def get_stop_lock_target(self, pos, price):
        progress = self.reward_progress(pos, price)
        current_stage = float(pos.get("stop_lock_stage", 0.0) or 0.0)
        stop_lock_steps = self._profile(pos)["stop_lock_steps"]

        best_stage = None
        for trigger_progress, lock_progress in stop_lock_steps:
            if progress >= trigger_progress and lock_progress > current_stage:
                best_stage = lock_progress

        return best_stage

    def apply_profit_lock(self, pos, lock_progress):
        old_stop = pos["stop"]
        entry = float(pos.get("entry", 0.0) or 0.0)
        take = float(pos.get("take", entry) or entry)

        if pos["side"] == "BUY":
            locked_stop = entry + (take - entry) * lock_progress
            pos["stop"] = max(old_stop, locked_stop)
        else:
            locked_stop = entry - (entry - take) * lock_progress
            pos["stop"] = min(old_stop, locked_stop)

        pos["stop_lock_stage"] = lock_progress
        pos["be_moved"] = pos["stop"] != old_stop or bool(pos.get("be_moved"))
        return old_stop, pos["stop"]

    def should_partial_close(self, pos, price):
        if pos.get("partial_done"):
            return False
        tp1 = pos.get("tp1")
        if tp1 is not None:
            if pos["side"] == "BUY" and price >= tp1:
                return True
            if pos["side"] == "SELL" and price <= tp1:
                return True
        return self.reward_progress(pos, price) >= self._profile(pos)["partial_tp_trigger"]

    def get_partial_fraction(self, pos=None):
        if pos is None:
            return self.default_profile["partial_close_fraction"]
        return self._profile(pos)["partial_close_fraction"]

    def should_activate_trailing(self, pos, price):
        # trail только после частичного закрытия или почти у цели
        if not pos.get("partial_done"):
            return False
        return self.progress_to_take(pos, price) >= self._profile(pos)["trail_trigger"]

    def apply_trailing(self, pos, price):
        old_stop = pos["stop"]
        profile = self._profile(pos)
        atr_gap_pct = max(profile["trail_gap_pct"], float(pos.get("atr_pct", 0.0)) * profile["atr_trail_mult"])

        if pos["side"] == "BUY":
            new_stop = price * (1 - atr_gap_pct)
            pos["stop"] = max(old_stop, new_stop, pos["entry"])
        else:
            new_stop = price * (1 + atr_gap_pct)
            pos["stop"] = min(old_stop, new_stop, pos["entry"])

        pos["trail_active"] = True
        return old_stop, pos["stop"]

    def hold_seconds_for_position(self, pos):
        signal_class = pos.get("signal_class", "REJECT")
        score = pos.get("signal_score", 0.0)

        if signal_class in {"A", "B", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"} or score >= self.strong_signal_score:
            return self.strong_hold_seconds

        return self.min_hold_seconds

    def should_early_exit_no_followthrough(self, pos, price):
        if not self.early_exit_enabled:
            return False

        if pos.get("partial_done"):
            return False

        signal_class = pos.get("signal_class", "REJECT")
        # A/B сделки не закрываем early-exit: ждём SL/TP/BE/trail
        if signal_class in {"A", "B", "BASE_A", "REVERSAL_A", "REVERSAL_DIV", "OB_A", "PATTERN_A"}:
            return False

        opened_at = pos.get("opened_at", 0)
        if opened_at <= 0:
            return False

        elapsed = __import__("time").time() - opened_at

        if signal_class == "C":
            if elapsed < self.early_exit_check_seconds_c:
                return False
            progress = self.progress_to_take(pos, price)
            pnl = self.unrealized_pnl(pos, price)
            return progress < self.early_exit_min_progress_c and pnl <= 0

        # REJECT / всё остальное
        if elapsed < self.early_exit_check_seconds_reject:
            return False

        progress = self.progress_to_take(pos, price)
        pnl = self.unrealized_pnl(pos, price)
        return progress < self.early_exit_min_progress_reject and pnl <= 0

    def should_take_liquidity_target(self, pos, price):
        target = pos.get("liquidity_target")
        if target is None:
            return False

        if pos["side"] == "BUY":
            return price >= target
        return price <= target

    def should_exit_on_adverse_flow(self, pos, orderflow_bias=0.0, oi_bias=0.0):
        # агрессивный выход по потоку только для слабых сетапов
        if pos.get("signal_class") not in {"C", "REJECT"}:
            return False

        if pos["side"] == "BUY":
            return orderflow_bias < -0.35 and oi_bias < -0.10
        return orderflow_bias > 0.35 and oi_bias > 0.10
