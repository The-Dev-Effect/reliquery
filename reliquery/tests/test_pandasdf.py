import pytest
from .. import Relic
from reliquery.storage import FileStorage
import pandas as pd
import numpy as np
from pandas._testing import assert_frame_equal

d = {
    "one": pd.Series([1.0, 2.0, 3.0], index=["a", "b", "c"]),
    "two": pd.Series([1.0, 2.0, 3.0, 4.0], index=["a", "b", "c", "d"]),
}
df = pd.DataFrame(d)

@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_pandasdf")


def test_list_pandasdf_file_when_add_pandasdf(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    rq.add_pandasdf("dataframe1",pd.DataFrame(d))
    pandasdf_text_list = rq.list_pandasdf()
    assert len(pandasdf_text_list) > 0


def test_pandasdf_given_name(test_storage):

    rq = Relic(name="test", relic_type="test", storage=test_storage)

    comparison = pd.DataFrame(d)
    rq.add_pandasdf("dataframe",comparison)

    pandasdf_data = rq.get_pandasdf("dataframe")
    assert isinstance(pandasdf_data,pd.DataFrame)
    assert pandasdf_data is not None
    assert_frame_equal(pandasdf_data, comparison, check_dtype=False)