from io import BytesIO
from .. import Relic
import pytest
from ..storage import FileStorage
import numpy as np
import os


@pytest.fixture()
def test_storage(tmp_path):
    return FileStorage(root=tmp_path, name="test_image")


def test_image_added(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_img = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    try:
        with open(test_img, "rb") as f:
            rq.add_image("img_test.png", f.read())
    except Exception as e:
        print(e)
        assert False

    assert len(rq.list_images()) == 1
    assert rq.list_images()[0] == "img_test.png"


def test_get_image_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_img = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    try:
        with open(test_img, "rb") as f:
            rq.add_image("img_test.png", f.read())
    except Exception as e:
        print(e)
        assert False

    stream = rq.get_image("img_test.png")

    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "img_test.png"
