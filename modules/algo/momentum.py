import numpy as np
import pandas as pd
from scipy import stats
from modules.algo.core import ValueBasedAlgo


class MomentumAlgo(ValueBasedAlgo):
    def __init__(self, window: int = 20, **kwargs):
        super().__init__(indicator_type="momentum", window=window, **kwargs)
        self.window = window

    def prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        smoothed_data = data.rolling(window=self.window).mean()
        return smoothed_data.dropna()

    def calculate_values(self, data: pd.DataFrame) -> pd.DataFrame:

        def momentum(ts: np.ndarray) -> float:
            ts_normalized = (ts - np.mean(ts)) / np.std(ts)
            x = np.arange(len(ts_normalized))
            slope, _, r_value, _, _ = stats.linregress(x, ts_normalized)
            annualized_slope = slope * np.sqrt(252)
            score = annualized_slope * (r_value**2)
            return score

        momentum_scores = data.apply(momentum, raw=True)

        momentum_scores = np.clip(
            momentum_scores,
            momentum_scores.mean() - 3 * momentum_scores.std(),
            momentum_scores.mean() + 3 * momentum_scores.std(),
        )

        min_scores = momentum_scores.min()
        max_scores = momentum_scores.max()
        normalized_momentum = (momentum_scores - min_scores) / (max_scores - min_scores)

        return normalized_momentum
