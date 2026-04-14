"""
Lightweight online RL agent for scheduling decisions.

This module implements a linear Q-function approximator so we can train
online without heavy ML dependencies.
"""

import json
import os 
import random
from typing import Dict, List, Optional, Tuple


class LinearQAgent:
    """A simple epsilon-greedy linear Q-learning agent."""

    def __init__(
        self,
        feature_dim: int,
        learning_rate: float = 0.03,
        gamma: float = 0.92,
        epsilon: float = 0.25,
        epsilon_min: float = 0.03,
        epsilon_decay: float = 0.9995,
        random_seed: int = 42,
    ) -> None:
        self.feature_dim = feature_dim
        self.learning_rate = learning_rate
        self.gamma = gamma
        self.epsilon = epsilon
        self.epsilon_min = epsilon_min
        self.epsilon_decay = epsilon_decay
        self.rng = random.Random(random_seed)

        # Start near zero so early behavior is mostly exploratory.
        self.weights: List[float] = [0.0 for _ in range(feature_dim)]
        # Running count of TD updates — used to track experience across runs.
        self.total_updates: int = 0

    def q_value(self, features: List[float]) -> float:
        """Compute Q(s, a) = w^T x."""
        return sum(w * x for w, x in zip(self.weights, features))

    def select_action(self, action_features: Dict[object, List[float]]) -> Tuple[object, List[float]]:
        """
        Select an action via epsilon-greedy policy.

        Returns:
            (action_key, features_for_action)
        """
        actions = list(action_features.items())
        if not actions:
            raise ValueError("select_action called with no actions")

        if self.rng.random() < self.epsilon:
            return self.rng.choice(actions)

        return self._greedy_action(actions)

    def select_action_greedy(self, action_features: Dict[object, List[float]]) -> Tuple[object, List[float]]:
        """Always pick the highest Q-value action (no exploration)."""
        actions = list(action_features.items())
        if not actions:
            raise ValueError("select_action_greedy called with no actions")
        return self._greedy_action(actions)

    def _greedy_action(self, actions: List[Tuple[object, List[float]]]) -> Tuple[object, List[float]]:
        best_action, best_features = actions[0]
        best_q = self.q_value(best_features)
        for action, features in actions[1:]:
            q = self.q_value(features)
            if q > best_q:
                best_q = q
                best_action = action
                best_features = features
        return best_action, best_features

    def update(
        self,
        features: List[float],
        reward: float,
        next_best_q: float = 0.0,
        terminal: bool = False,
    ) -> None:
        """Perform one TD update on linear Q-weights."""
        prediction = self.q_value(features)
        target = reward if terminal else reward + self.gamma * next_best_q
        td_error = target - prediction

        alpha = self.learning_rate
        for i in range(self.feature_dim):
            self.weights[i] += alpha * td_error * features[i]

        # Decay exploration slowly during simulation.
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        self.total_updates += 1

    def save(self, path: str) -> None:
        """Persist weights, epsilon, and update count to disk (atomic write)."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        payload = {
            "feature_dim": self.feature_dim,
            "weights": self.weights,
            "epsilon": self.epsilon,
            "total_updates": self.total_updates,
        }
        tmp = path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(payload, fh)
        os.replace(tmp, path)  # atomic on POSIX and Windows NTFS

    @classmethod
    def from_file(
        cls,
        path: str,
        *,
        learning_rate: float = 0.015,
        gamma: float = 0.92,
        epsilon_min: float = 0.03,
        epsilon_decay: float = 0.9997,
        random_seed: int = 42,
    ) -> "LinearQAgent":
        """Restore a previously saved agent with reduced LR so it fine-tunes, not re-explores."""
        with open(path) as fh:
            data = json.load(fh)
        agent = cls(
            feature_dim=int(data["feature_dim"]),
            learning_rate=learning_rate,
            gamma=gamma,
            epsilon=max(epsilon_min, float(data.get("epsilon", epsilon_min))),
            epsilon_min=epsilon_min,
            epsilon_decay=epsilon_decay,
            random_seed=random_seed,
        )
        agent.weights = [float(w) for w in data["weights"]]
        agent.total_updates = int(data.get("total_updates", 0))
        return agent
