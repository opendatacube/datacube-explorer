"""
Unit test for regex product grouping
"""

import re

import pytest

from cubedash._utils import get_sorted_product_summaries


class FakeProduct:
    def __init__(self, name):
        self.name = name


@pytest.fixture()
def test_product_groupby_regex_list():
    groupby_regex_list = (
        (r"wofs|_wo_", "Water Observations"),
        (r"fc_", "Fractional Cover"),
        (r"geomedian", "Geomedians"),
        (r"tmad", "TMAD"),
        (r"_summary", "Summary"),
        (
            r"(fc|wofs|percentile)_albers|_nbart_geomedian_annual|nbart_tmad_annual|wofs_[^f].*_summary",
            "C2 - Deprecated",
        ),
    )

    return groupby_regex_list


@pytest.fixture()
def test_product_list():
    product_summaries = [
        (FakeProduct("fc_percentile_albers_seasonal"), ()),
        (FakeProduct("ls7_fc_albers"), ()),
        (FakeProduct("wofs_albers"), ()),
        (FakeProduct("ls5_nbart_geomedian_annual"), ()),
        (FakeProduct("ls5_nbart_tmad_annual"), ()),
        (FakeProduct("wofs_annual_summary"), ()),
        (FakeProduct("wofs_nov_mar_summary"), ()),
    ]

    return product_summaries


def test_group_by_regex(test_product_groupby_regex_list, test_product_list):
    regex_group = {}
    for regex, group in test_product_groupby_regex_list:
        regex_group[re.compile(regex)] = group.strip()

    assert len(regex_group) == 6

    # group using regex
    def regex_key(t):
        for regex, group in regex_group.items():
            if regex.search(t[0].name):
                return group
        return "other"

    key = regex_key

    grouped_product_summarise = get_sorted_product_summaries(test_product_list, key)

    assert len(grouped_product_summarise) == 4
    assert grouped_product_summarise[0][0] == "Water Observations"
    assert grouped_product_summarise[1][0] == "Fractional Cover"
    assert grouped_product_summarise[2][0] == "Geomedians"
    assert grouped_product_summarise[3][0] == "TMAD"
    # assert grouped_product_summarise[4][0] == 'Summary'
    # assert grouped_product_summarise[5][0] == 'C2 - Deprecated'


def test_reverse_group_by_regex(test_product_groupby_regex_list, test_product_list):
    """
    reverse the groupby regex config tuple
    """
    regex_group = {}
    for regex, group in test_product_groupby_regex_list[::-1]:
        regex_group[re.compile(regex)] = group.strip()

    assert len(regex_group) == 6

    # group using regex
    def regex_key(t):
        for regex, group in regex_group.items():
            if regex.search(t[0].name):
                return group
        return "other"

    key = regex_key

    grouped_product_summarise = get_sorted_product_summaries(test_product_list, key)

    assert len(grouped_product_summarise) == 1
    assert grouped_product_summarise[0][0] == "C2 - Deprecated"
