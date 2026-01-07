from io import BytesIO
import pytest
from .. import Relic
from reliquery.storage import FileStorage, StorageItemDoesNotExist
import os


@pytest.fixture
def test_storage(tmp_path):
    return FileStorage(str(tmp_path), "test_audio")


@pytest.fixture
def audio_obj():
    bytes_io = None
    with open(os.path.join(os.path.dirname(__file__), "audio.wav"), "rb") as image_file:
        bytes_io = BytesIO(image_file.read())

    return bytes_io


def test_list_audio_when_add_audio(test_storage, audio_obj):
    rq = Relic(name="test", relic_type="test", storage=test_storage)

    assert len(rq.list_audio()) == 0
    rq.add_audio(name="test-audio", audio_obj=audio_obj)

    audio_list = rq.list_audio()
    assert len(audio_list) == 1


def test_get_audio_given_audio_name(test_storage, audio_obj):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    rq.add_audio(name="test-audio-get", audio_obj=audio_obj)

    stream = rq.get_audio(name="test-audio-get")
    assert len(stream.read()) > 0
    assert stream.name.split("/")[-1] == "test-audio-get"


def test_remove_audio_given_name(test_storage, audio_obj):
    rq = Relic(name="test", relic_type="test", storage=test_storage)
    rq.add_audio("test-audio", audio_obj)
    assert len(rq.list_audio()) == 1

    rq.remove_audio("test-audio")
    assert len(rq.list_audio()) == 0
    with pytest.raises(StorageItemDoesNotExist):
        rq.get_audio("test-audio")
