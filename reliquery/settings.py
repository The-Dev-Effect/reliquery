import os
import json


def get_config(relics_dir):
    # First we check the environment for the config
    if "ELEMENT_CONFIG" in os.environ:
        return json.loads(os.environ["ELEMENT_CONFIG"])

    # Otherwise, fallback on a file.
    config_path = os.path.join(relics_dir, "config")
    
    if not os.path.exists(config_path):
        os.makedirs(relics_dir)
        config_path = os.path.join(relics_dir, "config")
        with open(config_path, mode="w+") as config_file:
            config = {"storage": {"type": "File"}}
            config_file.write(json.dumps(config, indent=4))

    with open(config_path, mode="r") as config_file:
        config = json.load(config_file)

    return config