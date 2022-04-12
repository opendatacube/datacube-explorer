"""
Unit test for regex product grouping
"""

import pytest
import re
import itertools
from datetime import datetime

from cubedash._pages import _get_grouped_products
from cubedash.summary._stores import ProductSummary

class fakeproduct:
    def __init__(self, name):
        self.name = name

@pytest.fixture()
def test_product_groupby_regex_list():
    CUBEDASH_PRODUCT_GROUP_BY_REGEX = (
          (r"wofs|_wo_", "Water Observations"),
          (r"fc_", "Fractional Cover"),
          (r"geomedian", "Geomedians"),
          (r"tmad", "TMAD"),
          (r"_summary", "Summary"),
          (r"(fc|wofs|percentile)_albers|_nbart_geomedian_annual|nbart_tmad_annual|wofs_[^f].*_summary", "C2 - Deprecated"),
    )

    return CUBEDASH_PRODUCT_GROUP_BY_REGEX

@pytest.fixture()
def test_product_list():
    product_summaries = [
        (fakeproduct('fc_percentile_albers_seasonal'),()),
        (fakeproduct('ls7_fc_albers'),()),
        (fakeproduct('wofs_albers'),()),
        (fakeproduct('ls5_nbart_geomedian_annual'),()),
        (fakeproduct('ls5_nbart_tmad_annual'),()),
        (fakeproduct('wofs_annual_summary'),()),
        (fakeproduct('wofs_nov_mar_summary'),())
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
        return _DEFAULT_GROUP_NAME

    key = regex_key

    grouped_product_summarise = sorted(
        (
            (name or "", list(items))
            for (name, items) in itertools.groupby(
                sorted(test_product_list, key=key), key=key
            )
        ),
        # Show largest groups first
        key=lambda k: len(k[1]),
        reverse=True,
    )

    assert len(grouped_product_summarise) == 4
    assert grouped_product_summarise[0][0] == 'Water Observations'
    assert grouped_product_summarise[1][0] == 'Fractional Cover'
    assert grouped_product_summarise[2][0] == 'Geomedians'
    assert grouped_product_summarise[3][0] == 'TMAD'
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
        return _DEFAULT_GROUP_NAME

    key = regex_key

    grouped_product_summarise = sorted(
        (
            (name or "", list(items))
            for (name, items) in itertools.groupby(
                sorted(test_product_list, key=key), key=key
            )
        ),
        # Show largest groups first
        key=lambda k: len(k[1]),
        reverse=True,
    )

    assert len(grouped_product_summarise) == 1
    assert grouped_product_summarise[0][0] == 'C2 - Deprecated'