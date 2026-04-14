"""
Predictive analytics utilities for RL+PA scheduling.

Maintains short-term workload signals and provides simple forecasts used by
scheduler reward shaping and state features.
"""

from collections import deque
from typing import Deque, Optional, Tuple


class PredictiveAnalyticsModel:
    """Online forecaster for arrival pressure and queue pressure."""

    def __init__(self, history_size: int = 120, ema_alpha: float = 0.2) -> None:
        self.history_size = history_size
        self.ema_alpha = ema_alpha
        self.history: Deque[Tuple[float, int, int, float]] = deque(maxlen=history_size)

        self._ema_arrival_rate = 0.0
        self._ema_arrival_rate_sq = 0.0
        self._ema_queue_len = 0.0
        self._initialized = False

    def observe(
        self,
        time_now: float,
        queue_len: int,
        total_arrivals: int,
        avg_server_utilization: float,
    ) -> None:
        """Record a snapshot and update EMA predictors."""
        self.history.append((time_now, queue_len, total_arrivals, avg_server_utilization))

        if not self._initialized:
            self._ema_queue_len = float(queue_len)
            self._ema_arrival_rate = 0.0
            self._initialized = True
            return

        self._ema_queue_len = (
            self.ema_alpha * float(queue_len)
            + (1.0 - self.ema_alpha) * self._ema_queue_len
        )

        if len(self.history) >= 2:
            t_prev, _, arrivals_prev, _ = self.history[-2]
            dt = max(1e-6, time_now - t_prev)
            inst_rate = max(0.0, (total_arrivals - arrivals_prev) / dt)
            self._ema_arrival_rate = (
                self.ema_alpha * inst_rate
                + (1.0 - self.ema_alpha) * self._ema_arrival_rate
            )
            self._ema_arrival_rate_sq = (
                self.ema_alpha * inst_rate * inst_rate
                + (1.0 - self.ema_alpha) * self._ema_arrival_rate_sq
            )

    def arrival_rate_cv(self) -> float:
        """Coefficient of variation of arrival rate (std/mean).

        Higher values indicate oscillating arrival patterns (e.g. time-varying).
        Low values indicate steady arrival (e.g. poisson).
        """
        variance = max(0.0, self._ema_arrival_rate_sq - self._ema_arrival_rate ** 2)
        mean = max(1e-9, self._ema_arrival_rate)
        return (variance ** 0.5) / mean

    def predicted_arrival_rate(self) -> float:
        """Estimated near-future arrival rate in requests per sim time-unit."""
        return max(0.0, self._ema_arrival_rate)

    def predicted_queue_len(self) -> float:
        """Estimated near-future queue length."""
        return max(0.0, self._ema_queue_len)

    def predicted_queue_pressure(self, active_server_count: int, total_server_count: int) -> float:
        """
        Estimate queue pressure normalized to [0, 1+].

        Pressure rises when queue is high and active server pool is small.
        """
        active_frac = active_server_count / max(1, total_server_count)
        # A queue of 20 with all servers active is moderate pressure.
        queue_term = self.predicted_queue_len() / 20.0
        capacity_term = 1.0 + (1.0 - active_frac)
        return max(0.0, queue_term * capacity_term)

    def predicted_renewable_fraction(self, time_now: float, cycle: float = 240.0) -> float:
        """
        Synthetic renewable availability proxy in [0.2, 0.9].

        Uses a day-like sinusoidal pattern over simulation time.
        """
        # Keep dependency-free sinusoidal approximation using two-point piecewise ramps.
        phase = (time_now % cycle) / cycle
        if phase < 0.25:
            return 0.2 + (phase / 0.25) * 0.7
        if phase < 0.75:
            return 0.9 - ((phase - 0.25) / 0.5) * 0.3
        return 0.6 - ((phase - 0.75) / 0.25) * 0.4
