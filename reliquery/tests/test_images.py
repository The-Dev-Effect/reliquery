from .. import Relic
import pytest
from ..storage import FileStorage, StorageItemDoesNotExist
import os
from io import BytesIO


@pytest.fixture()
def test_storage(tmp_path):
    return FileStorage(root=tmp_path, name="test_image")


def test_image_added(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    with open(
        os.path.join(os.path.dirname(__file__), "ideal-engineer.png"), "rb"
    ) as image_file:
        bytesIO = BytesIO(image_file.read())

    rq.add_image("img_test.png", bytesIO)

    assert len(rq.list_images()) == 1
    assert rq.list_images()[0] == "img_test.png"


def test_image_added_from_path(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_img = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    rq.add_image_from_path("img_test.png", test_img)

    assert len(rq.list_images()) == 1
    assert rq.list_images()[0] == "img_test.png"


def test_save_image_to_path(test_storage, tmp_path):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_file = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    rq.add_image_from_path("img_test.png", test_file)
    path_to_save = os.path.join(tmp_path, "imgtestsaved.png")

    assert os.path.exists(path_to_save) is False
    rq.save_image_to_path("img_test.png", path_to_save)
    assert os.path.exists(path_to_save)


def test_get_image_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_img = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    rq.add_image_from_path("img_test.png", test_img)

    stream = rq.get_image("img_test.png")

    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "img_test.png"


def test_remove_image_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_img = os.path.join(os.path.dirname(__file__), "ideal-engineer.png")
    rq.add_image_from_path("img_test.png", test_img)

    assert len(rq.list_images()) == 1

    rq.remove_image("img_test.png")
    assert len(rq.list_images()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_image("img_test.png")
