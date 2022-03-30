# Reliquery
![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/The-Dev-Effect/reliquery?include_prereleases)
[![example workflow](https://github.com/The-Dev-Effect/reliquery/actions/workflows/main.yml/badge.svg)](https://github.com/The-Dev-Effect/reliquery/actions/workflows/main.yml)
## Science's Artifact Antiformat
An anti-format storage tool aimed towards supporting scientists. Giving them the ability to store data how they want and where they want. Simplifying the storage of research materials making them easy to find and easy to share.

## Table of Contents
1. [Production](#prod)
2. [Development](#dev)
    1. [Local install](#loc-ins)
3. [Example](#quick)
4. [HTML](#html)
5. [Images](#img)
6. [JSON](#json)
7. [Pandas DataFrame](#pd)
8. [Files](#files)
9. [Jupyter Notebooks](#notebooks)
10. [Query Relics](#query)
11. [Config](#config)
12. [File Storage](#file)
13. [S3 Storage](#s3)
14. [Dropbox](#dropbox)
15. [Google Drive](#googledrive)
16. [Google Cloud](#googlecloud)
17. [License](#lic)

## For production<a name="prod"></a>
latest version 0.2.6
```
pip install reliquery
```

## For development<a name="dev"></a>

### Local Install<a name="loc-ins"></a>
```
cd reliquery
pip install -e .
```
### Quick Example Usage<a name="quick"></a>
```python
from reliquery import Relic
import numpy as np
from IPython.display import HTML, Image
 
r = Relic(name="quick", relic_type="tutorial")
ones_array = np.ones((10, 10))
r.add_array("ones", ones_array)
np.testing.assert_array_equal(r.get_array("ones"), ones_array)
r.add_text("long_form", "Some long form text. This is something we can do NLP on later")
r.add_tag({"pass": "yes"})
r.add_json("json", {"One":1, "Two": 2, "Three": 3})
print(r.describe())
```
### HTML supported<a name="html"></a>
Add HTML as a string:
```python
# Example
r.add_html_string("welcome", "<div><p>Hello, World</p></div>")
```
Add HTML from a file path:
```python
# Example
r.add_html_from_path("figures", <path to html file>)
```
Get and display HTML using Reliquery:
```python
# Read only S3 demo
r_demo = Relic(name="intro", relic_type="tutorial", storage_name="demo")
print(r_demo.list_html())
display(HTML(r_demo.get_html('nnmf2 resnet34.html')))
```
### Images supported<a name="img"></a>
Add images by passing images as bytes:
```python
# Example
with open("image.png", "rb") as f:
    r.add_image("image-0.png", f.read())
```
List images:
```python
print(r_demo.list_images())
display(Image(r_demo.get_image("reliquery").read()))
```

Get images:
```python
r_demo.get_image("reliquery")
```

Display PIL image:
```python
r_demo.get_pil_image("reliquery")
```

### JSON supported<a name="json"></a>
Add json by passing it in as a dictionary:
```python
# Example
r.add_json("json", {"First": 1, "Second": 2, "Third":3})
```

List json 
```python
r.list_json()
```

Get json by taking the name and returning the dictionary
```python 
r.get_json("json")
```

### Pandas DataFrame<a name="pd"></a>
Note that json is used to serialize which comes with other caveats that can be found here: https://pandas.pydata.org/pandas-docs/version/0.23/generated/pandas.DataFrame.to_json.html
```python
#Example
d = {
    "one": pd.Series([1.0, 2.0, 3.0], index=["a", "b", "c"]),
    "two": pd.Series([1.0, 2.0, 3.0, 4.0], index=["a", "b", "c", "d"]),
}
df = pd.DataFrame(d)
r.add_pandasdf("pandasdf", df)

List pandasdf
r.list_pandasdf()

Get pandas dataframe by taking the name 
r.get_pandasdf("pandasdf")
```

### Files <a name="files"></a>
```python
#Example
r.add_files_from_path("TestFileName", test_file_path)

List files
r.list_files()

Get file 
r.get_file("TestFileName")

Save file 
r.save_files_to_path("TestFile", path_to_save)
```

### Jupyter Notebooks<a name="notebooks"></a>
```python
#Example
test_notebook = os.path.join(os.path.dirname(__file__), "notebook_test.ipynb")
r.add_notebook_from_path("TestNotebook", test_notebook)

List Notebooks
notebook_list = r.list_notebooks()

Get Notebook
r.get_notebook("TestNotebook")

Save Notebook to path
path_to_save = os.path.join(tmp_path, "testnotebook.ipynb")
r.save_notebook_to_path("TestNotebook", path_to_save)

View Notebooka via HTML
r.get_notebook_html(TestNotebook)
```

### Query Relics<a name="query"></a>
```python
from reliquery import Reliquery

rel = Reliquery()

relics = rel.get_relics_by_tag("pass", "yes")

relics[0].describe()
```


### Config<a name="config"></a>
An optional json text file named config can be created in ~/reliquery to customize storage.
<br />
It can be setup to look like this...
```json
{
  "default": {
    "storage": {
      "type": "File",
      "args": {
        "root": "ENTER_PATH_TO_STORE_YOUR_RELIQUERY"
      }
    }
  },
  "demo": {
    "storage": {
      "type": "S3",
      "args": {
        "s3_signed": false,
        "s3_bucket": "reliquery",
        "prefix": "relics"
      }
    }
  }
}
```

## File Storage<a name="file"></a>
With this configuration, the relic will be persisted to:
<br />
/home/user/reliquery/relic_type/relic_name/data_type/data_name
<br />
In the quick example that will be:
<br />
/home/user/reliquery/reliquery/basic/relic_tutorial/arrays/ones
<br />

## S3 Storage<a name="s3"></a>
s3_signed
* true = uses current aws_cli configuration
* false = uses the anonymous IAM role

## Dropbox Storage<a name="dropbox"></a>
To use Dropbox with reliquery, the following must be added to the config file in reliquery
```json
"Dropbox":{
        "storage": {
            "type": "Dropbox",
            "args": {
                "access_token": "YOUR_ACCESS_TOKEN",
                "prefix": "relics"
            }
        }
    }
```
The access token is obtained by creating an app in the Dropbox App Console and setting the following permissions:
* accoung_info.read
* files.metadata.write
* files.metadata.read
* files.content.write
* files.content.read
* file_requests.write
* file_requests.read
Dropbox App Console([https://www.dropbox.com/developers/apps/create](https://www.dropbox.com/developers/apps/create?_tk=pilot_lp&_ad=ctabtn1&_camp=create))

## Google Drive Storage<a name="googledrive"></a>
To use Google Drive with reliquery, the following must be added to the config file in reliquery
```json
"GoogleDrive":{
        "storage": {
            "type": "GoogleDrive",
            "args": {
                "token_file": "TOKEN_FILE_PATH",
                "prefix": "relics",
                "SCOPES": ["https://www.googleapis.com/auth/drive"],
                "shared_folder_id": "SHARED_FOLDER_ID"
            }
        }
    }
```
To obtain a token file, you must create a key from a service account in the Google Cloud Platform and download it as a json file. You then need to share a folder with the service account and obtain the folder id which is the last part of the URL when you are in the folder.
Google Cloud Platform([https://console.cloud.google.com/iam-admin/serviceaccounts](https://console.cloud.google.com/iam-admin/serviceaccounts))

## Google Cloud Storage<a name="googlecloud"></a>
To use Google Cloud with reliquery, the following must be added to the config file in reliquery
```json
"GoogleCloud":{
        "storage": {
            "type": "GoogleCloud",
            "args": {
                "token_file": "TOKEN_FILE_PATH",
                "prefix": "relics",
                "root": "reliquery"
            }
        }
    }
```
To obtain a token file, you must create a key from a service account in the Google Cloud Platform and download it as a json file. 
Google Cloud Platform([https://console.cloud.google.com/iam-admin/serviceaccounts](https://console.cloud.google.com/iam-admin/serviceaccounts))

## License<a name="lic"></a>

Reliquery is free and open source! All code in this repository is dual-licensed under either:

* MIT License ([LICENSE-MIT](docs/LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))
* Apache License, Version 2.0 ([LICENSE-APACHE](docs/LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

at your option. This means you can select the license you prefer.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
