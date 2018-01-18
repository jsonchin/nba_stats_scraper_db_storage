import yaml
CONFIG = {}

def load_config():
    """
    Loads the configurations located in the config file
    at config/config.yaml.
    """
    with open('config/config.yaml', 'r') as f:
        config_yaml = yaml.load(f)
        for config_key in config_yaml:
            CONFIG[config_key] = config_yaml[config_key]
