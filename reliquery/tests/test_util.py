from reliquery import Relic
from reliquery.storage import FileStorage


def init_reliquery_test_data(tmp_path) -> None:
    stor1 = FileStorage(tmp_path.joinpath("one"), "stor1")
    stor2 = FileStorage(tmp_path.joinpath("two"), "stor2")

    rq1 = Relic("test1", "test", storage=stor1, storage_name="stor1")
    rq2 = Relic("test2", "test", storage=stor2, storage_name="stor2")

    rq1.add_text("rq1", "first relic")
    rq1.add_tag({"go-no-go": "go"})

    rq2.add_text("rq2", "second relic")
    rq2.add_tag({"go-no-go": "no-go"})

    return [stor1, stor2]
