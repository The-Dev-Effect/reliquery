import os
import json


def get_config(reliquery_dir):
    # First we check the environment for the config
    if "RELIQUERY_CONFIG" in os.environ:
        return json.loads(os.environ["RELIQUERY_CONFIG"])

    # Otherwise, fallback on a file.
    config_path = os.path.join(reliquery_dir, "config")
    print(f"config path: \n\t{config_path}\nexists:\n\t{os.path.exists(config_path)}")
    print(F"Check hardcoded path is avaiable(/home/ubuntu/reliquery/config):\n\t{os.path.exists("/home/ubuntu/reliquery/config")} ")
    print(F"Check hardcoded path is avaiable(/home/ubuntu/reliquery/reliquery/config):\n\t{os.path.exists("/home/ubuntu/reliquery/reliquery/config")} ")
    if os.path.exists(config_path):
        with open(config_path, mode="r") as config_file:
            config = json.load(config_file)
        return config

    return {
        "default": {
            "storage": {
                "type": "File",
                "args": {
                    "root": reliquery_dir,
                },
            }
        },
        "demo": {
            "storage": {
                "type": "S3",
                "args": {
                    "s3_signed": False,
                    "s3_bucket": "reliquery",
                    "prefix": "relics",
                },
            }
        },
    }
