import os
import pytest
from reliquery.relic import Relic
from ..storage import FileStorage
from .. import Reliquery
from .test_util import init_reliquery_test_data


def test_querying_relics(tmp_path):
    storages = init_reliquery_test_data(tmp_path)

    rel = Reliquery(storages=storages)

    relics = rel.query("select * from relics")

    assert len(relics) == 2


def test_query_relics_by_tag(tmp_path):

    storages = init_reliquery_test_data(tmp_path)

    rel = Reliquery(storages=storages)

    r = rel.get_relics_by_tag("go-no-go", "go")
    assert len(r) == 1

    rq = r[0]

    assert isinstance(rq.storage, FileStorage)
    assert rq.storage.name == "stor1"
    assert rq.name == "test1"
    assert rq.relic_type == "test"
    assert rq.get_text("rq1") == "first relic"


def test_remove_relic_given_relic(tmp_path):
    relic_path = tmp_path.joinpath("three")
    storages = init_reliquery_test_data(tmp_path)

    # test storage & relic
    storage = FileStorage(tmp_path.joinpath("three"), "stor3")
    storages.append(storage)
    relic = Relic(name="test3", relic_type="test", storage=storage)

    rel = Reliquery(storages=storages)
    assert len(rel.get_relic_names()) == 3
    assert os.listdir(os.path.join(relic_path, "test", "test3"))

    rel.remove_relic(relic)

    assert len(rel.get_relic_names()) == 2
    with pytest.raises(FileNotFoundError):
        os.listdir(os.path.join(relic_path, "test", "test3"))
