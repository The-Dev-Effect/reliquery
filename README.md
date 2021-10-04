# Reliquery
Science's Artifact Antiformat

### Quick Example Usage
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
print(r.describe())

# Read only S3 demo
r_demo = Relic(name="intro", relic_type="tutorial", storage_name="demo")
print(r_demo.list_html())
display(HTML(r_demo.get_html('nnmf2 resnet34.html')))

print(r_demo.list_images())
display(Image(r_demo.get_image("reliquery")))
```

### Config
A json text file named config located in ~/reliquery
<br />
Default looks like...
```json
{
  "default": {
    "storage": {
      "type": "File",
      "args": {
        "root": "/home/user/reliquery"
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
### File Storage
With this configuration, the relic will be persisted to:
<br />
/home/user/reliquery/relic_type/relic_name/data_type/data_name
<br />
In the quick example that will be:
<br />
/home/user/reliquery/reliquery/basic/relic_tutorial/arrays/ones
<br />

### S3 Storage
s3_signed
* true = uses current aws_cli configuration
* false = uses the anonymous IAM role

## License

Reliquery is free and open source! All code in this repository is dual-licensed under either:

* MIT License ([LICENSE-MIT](docs/LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))
* Apache License, Version 2.0 ([LICENSE-APACHE](docs/LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

at your option. This means you can select the license you prefer.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
