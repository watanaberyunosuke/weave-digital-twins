"""Scoring a DMA's residual against its own baseline (HDH-120).

    python3 -m unittest discover -s model -p 'test_*.py'
"""

import statistics
import unittest

from residuals import classify

SETTINGS = {"min_samples": 24, "z_watch": 2.0, "z_leak": 3.0, "sigma_floor": 0.1}


def score(residual, history):
    """classify() with the baseline computed the way the SQL does."""
    n = len(history)
    mean = statistics.fmean(history) if history else None
    sd = statistics.stdev(history) if n > 1 else None
    return classify(residual, n, mean, sd, **SETTINGS)


class ClassifyTest(unittest.TestCase):

    def test_a_constant_offset_is_not_flagged_once_its_baseline_exists(self):
        # The model reads 4 m high in this DMA every solve: that is the model's
        # bias, not a leak.
        result = score(4.0, [4.0] * 48)
        self.assertEqual(result.status, "OK")
        self.assertFalse(result.flagged)
        self.assertAlmostEqual(result.z_score, 0.0)

    def test_a_step_of_three_baseline_deviations_is_flagged(self):
        history = [4.0 + (0.5 if i % 2 else -0.5) for i in range(48)]
        sd = statistics.stdev(history)
        result = score(4.0 + 3 * sd, history)
        self.assertEqual(result.status, "SUSPECTED_LEAK")
        self.assertTrue(result.flagged)
        self.assertAlmostEqual(result.z_score, 3.0)

    def test_insufficient_history_is_unknown_not_ok(self):
        for history in ([], [4.0] * 23):
            result = score(4.0, history)
            self.assertEqual(result.status, "UNKNOWN")
            self.assertIsNone(result.z_score)
            self.assertFalse(result.flagged)

    def test_between_the_thresholds_is_watch(self):
        history = [4.0 + (0.5 if i % 2 else -0.5) for i in range(48)]
        sd = statistics.stdev(history)
        result = score(4.0 + 2.5 * sd, history)
        self.assertEqual((result.status, result.flagged), ("WATCH", False))

    def test_a_drop_is_scored_like_a_rise(self):
        history = [4.0 + (0.5 if i % 2 else -0.5) for i in range(48)]
        sd = statistics.stdev(history)
        self.assertEqual(score(4.0 - 3 * sd, history).status, "SUSPECTED_LEAK")

    def test_a_perfectly_steady_baseline_uses_the_floor(self):
        # sd is 0: without the floor a 5 cm change would be an infinite z.
        result = score(4.05, [4.0] * 48)
        self.assertAlmostEqual(result.z_score, 0.5)
        self.assertEqual(result.status, "OK")
        self.assertEqual(score(4.3, [4.0] * 48).status, "SUSPECTED_LEAK")

    def test_missing_deviation_falls_back_to_the_floor(self):
        result = classify(4.2, 30, 4.0, None, **SETTINGS)
        self.assertAlmostEqual(result.z_score, 2.0)
        self.assertEqual(result.status, "WATCH")
        self.assertEqual(classify(4.2, 30, 4.0, float("nan"), **SETTINGS).status, "WATCH")


if __name__ == "__main__":
    unittest.main()
