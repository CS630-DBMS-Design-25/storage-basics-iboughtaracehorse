import unittest
import math
from Additional import *

class TestDBMSFunctions(unittest.TestCase):

    def setUp(self):
        self.horses = [
            ["id", "name", "stable_id", "speed"],
            ["1", "Thunder", "10", "45"],
            ["2", "Lightning", "20", "50"],
            ["3", "Blaze", "10", "48"],
        ]

        self.stables = [
            ["stable_id", "stable_name"],
            ["10", "Blue Ribbon"],
            ["20", "Golden Hoof"],
        ]

    def test_merge_join(self):
        joined = merge_join(self.horses, self.stables, "stable_id", "stable_id")
        self.assertEqual(joined[0], ["id", "name", "stable_id", "speed", "stable_name"])
        self.assertIn(["1", "Thunder", "10", "45", "Blue Ribbon"], joined)

    def test_aggregate_sum(self):
        result = aggregate_sum(self.horses, "speed")
        self.assertAlmostEqual(result, 143.0)

    def test_aggregate_avg(self):
        result = aggregate_avg(self.horses, "speed")
        self.assertAlmostEqual(result, 47.666666666666664)

    def test_aggregate_min(self):
        result = aggregate_min(self.horses, "speed")
        self.assertEqual(result, 45.0)

    def test_aggregate_max(self):
        result = aggregate_max(self.horses, "speed")
        self.assertEqual(result, 50.0)

    def test_scalar_abs(self):
        result = scalar_abs(self.horses, "speed")
        speeds = [float(row[3]) for row in result[1:]]
        self.assertListEqual(speeds, [45.0, 50.0, 48.0])

    def test_scalar_sqrt(self):
        result = scalar_sqrt(self.horses, "speed")
        expected = [math.sqrt(45), math.sqrt(50), math.sqrt(48)]
        speeds = [float(row[3]) for row in result[1:]]
        for s, e in zip(speeds, expected):
            self.assertAlmostEqual(s, e)

    def test_scalar_pow(self):
        power = 3
        result = scalar_pow(self.horses, "speed", power)
        expected = [pow(45, power), pow(50, power), pow(48, power)]
        speeds = [float(row[3]) for row in result[1:]]
        for s, e in zip(speeds, expected):
            self.assertAlmostEqual(s, e)

if __name__ == "__main__":
    unittest.main()
