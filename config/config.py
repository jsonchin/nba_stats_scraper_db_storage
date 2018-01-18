import yaml

def load_config(file_name):
    """
    Loads the configurations located in the config file
    at config/config.yaml
    into the CONFIG dictionary.
    """
    CONFIG = {}
    with open(file_name, 'r') as f:
        config_yaml = yaml.load(f)
        for config_key in config_yaml:
            CONFIG[config_key] = config_yaml[config_key]
    return CONFIG
