import pytest
from .. import Relic
from reliquery.storage import FileStorage, StorageItemDoesNotExist
import os
from io import BytesIO


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_videos")


def test_video_added(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    with open(
        os.path.join(os.path.dirname(__file__), "catvideo.mp4"), "rb"
    ) as image_file:
        bytesIO = BytesIO(image_file.read())

    rq.add_image("video_test.mp4", bytesIO)

    assert len(rq.list_images()) == 1
    assert rq.list_images()[0] == "video_test.mp4"


def test_video_added_from_path(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_video = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_image_from_path("video_test.mp4", test_video)

    assert len(rq.list_images()) == 1
    assert rq.list_images()[0] == "video_test.mp4"


def test_save_video_to_path(test_storage, tmp_path):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_file = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_image_from_path("video_test.mp4", test_file)
    path_to_save = os.path.join(tmp_path, "videotestsaved.png")

    assert os.path.exists(path_to_save) is False
    rq.save_image_to_path("video_test.mp4", path_to_save)
    assert os.path.exists(path_to_save)


def test_get_image_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_img = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_image_from_path("video_test.mp4", test_img)

    stream = rq.get_image("video_test.mp4")

    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "video_test.mp4"


def test_remove_image_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_img = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_image_from_path("video_test.mp4", test_img)

    assert len(rq.list_images()) == 1

    rq.remove_image("video_test.mp4")
    assert len(rq.list_images()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_image("video_test.mp4")
