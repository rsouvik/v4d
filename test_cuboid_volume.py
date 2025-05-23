
from cuboid_volume import vol_cuboid
import unittest

class TestCuboidVolume(unittest.TestCase):
    def test_vol_cuboid(self):
        # Test with a positive integer
        self.assertEqual(vol_cuboid(3), 27)
        
        # Test with a negative integer
        self.assertEqual(vol_cuboid(-3), -27)
        
        # Test with zero
        self.assertEqual(vol_cuboid(0), 0)
        
        # Test with a float
        self.assertAlmostEqual(vol_cuboid(2.5), 15.625)