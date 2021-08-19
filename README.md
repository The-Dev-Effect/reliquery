# Reliquery
Science's Artifact Antiformat

### Example Usage
```python
from reliquery import Relic
import numpy as np

r = Relic(name="tutorial", relic_type="basic")
ones_array = np.ones((10, 10))
r.add_array("ones", ones_array)
np.testing.assert_array_equal(r.get_array("ones"), ones_array)
```

### Config
A json text file named config located in {home_dir}/reliquery
<br />
Looks like...
```json
{
  "storage": {
    "type": "S3",
    "args": {
      "s3_bucket": "123qwe123ad",
      "prefix": "rel"
    }
  }
}
```
### Local Storage
The relic will be persisted to:
<br />
{home_dir}/reliquery/relic_type/relic_name/data_type/data_name
<br />
In this example that will be:
<br />
{home_dir}/reliquery/basic/relic_tutorial/arrays/ones
<br />

## License

Reliquery is free and open source! All code in this repository is dual-licensed under either:

* MIT License ([LICENSE-MIT](docs/LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))
* Apache License, Version 2.0 ([LICENSE-APACHE](docs/LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))

at your option. This means you can select the license you prefer.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
