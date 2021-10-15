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

    r = rel.get_relics_by_tag({"go-no-go": "go"})

    assert isinstance(r, FileStorage)
