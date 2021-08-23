
import os
import reliquery.relic as relic

def test_list_html_file_when_add_html(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)

    rq = relic.Relic(name="element-bucket", relic_type="test")

    rq.add_html("test html", "/home/welsh/Workspace/projects/reliquery/reliquery/tests/test.html")
    html_list = rq.list_html()
    assert len(html_list) > 0
    
def test_html_file_given_file_name(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)
    
    rq = relic.Relic(name="element-bucket", relic_type="test")

    rq.add_html("test html", "/home/welsh/Workspace/projects/reliquery/reliquery/tests/test.html")

    html = rq.get_html("test html")
    assert html != None
    assert html.startswith("<div>")

def test_list_html_files(tmpdir):
    reliquery_dir = os.path.join(tmpdir, "reliquery")
    os.makedirs(reliquery_dir)

    rq = relic.Relic(name="element-bucket", relic_type="test")

    rq.add_html("test html", "/home/welsh/Workspace/projects/reliquery/reliquery/tests/test.html")
    rq.add_html("test-html", "/home/welsh/Workspace/projects/reliquery/reliquery/tests/test.html")
    html_list = rq.list_html()
    assert len(html_list) == 2