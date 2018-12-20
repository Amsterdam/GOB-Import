import unittest

from gobimport.converter import _apply_filters


class TestConverter(unittest.TestCase):

    def setUp(self):
        pass

    def test_apply_filters(self):
        filters = [
            ["re.sub", "a", "b"],
            ["upper"]
        ]
        result = _apply_filters("a", filters)
        self.assertEqual(result, "B")

        filters = [
            ["upper"],
            ["re.sub", "a", "b"],
        ]
        result = _apply_filters("a", filters)
        self.assertEqual(result, "A")
