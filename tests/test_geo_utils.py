"""
Tests for geospatial utility functions.
"""

import pytest
import math

from utils.geo_utils import haversine_distance, estimate_is_moving


class TestHaversineDistance:

    def test_same_point_returns_zero(self):
        dist = haversine_distance(29.5, -89.5, 29.5, -89.5)
        assert dist == pytest.approx(0.0, abs=0.001)

    def test_known_distance(self):
        # New York (40.7128, -74.0060) to London (51.5074, -0.1278)
        # Expected: ~3,459 nautical miles
        dist = haversine_distance(40.7128, -74.0060, 51.5074, -0.1278)
        assert 3400 < dist < 3500

    def test_equator_one_degree(self):
        # One degree of longitude at equator ~ 60 nautical miles
        dist = haversine_distance(0.0, 0.0, 0.0, 1.0)
        assert 59 < dist < 61

    def test_symmetric(self):
        d1 = haversine_distance(29.5, -89.5, 30.0, -88.0)
        d2 = haversine_distance(30.0, -88.0, 29.5, -89.5)
        assert d1 == pytest.approx(d2, abs=0.001)


class TestEstimateIsMoving:

    def test_stationary(self):
        assert estimate_is_moving(0.0) is False

    def test_slow_drift(self):
        assert estimate_is_moving(0.3) is False

    def test_moving(self):
        assert estimate_is_moving(5.0) is True

    def test_custom_threshold(self):
        assert estimate_is_moving(0.8, threshold=1.0) is False
        assert estimate_is_moving(1.5, threshold=1.0) is True
