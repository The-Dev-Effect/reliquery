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
    ) as video_file:
        bytesIO = BytesIO(video_file.read())

    rq.add_video("video_test.mp4", bytesIO)

    assert len(rq.list_videos()) == 1
    assert rq.list_videos()[0] == "video_test.mp4"


def test_video_added_from_path(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_video = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_video_from_path("video_test.mp4", test_video)

    assert len(rq.list_videos()) == 1
    assert rq.list_videos()[0] == "video_test.mp4"


def test_save_video_to_path(test_storage, tmp_path):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    test_file = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_video_from_path("video_test.mp4", test_file)
    path_to_save = os.path.join(tmp_path, "videotestsaved.png")

    assert os.path.exists(path_to_save) is False
    rq.save_video_to_path("video_test.mp4", path_to_save)
    assert os.path.exists(path_to_save)


def test_get_video_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_video = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_video_from_path("video_test.mp4", test_video)

    stream = rq.get_video("video_test.mp4")

    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "video_test.mp4"


def test_remove_video_given_name(test_storage):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    test_video = os.path.join(os.path.dirname(__file__), "catvideo.mp4")
    rq.add_video_from_path("video_test.mp4", test_video)

    assert len(rq.list_videos()) == 1

    rq.remove_video("video_test.mp4")
    assert len(rq.list_videos()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_video("video_test.mp4")
