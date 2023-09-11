from __future__ import print_function, division, absolute_import
from configparser import ConfigParser
import os

DEFAULT_CONFIG_FILE = "./config.ini"

__current_config: ConfigParser = None


def get_conf(filepath: str = DEFAULT_CONFIG_FILE, force: bool = False) -> ConfigParser:
    """
    Read config.ini file and return ConfigParser object

    Args:
        filepath (str): path to config.ini file (default: "./config.ini")
        force (bool): force to read config.ini file (default: False)

    Returns:
        conf (ConfigParser): ConfigParser object
    """
    global __current_config
    if __current_config is not None:
        return __current_config
    filepath = os.environ.get("CONFIG_FILE", filepath)
    if os.path.exists(filepath) is False:
        raise FileNotFoundError(f"File not found: {filepath}")
    conf = ConfigParser()
    conf.read(filepath)
    __current_config = conf
    return conf


def conf_s3() -> [dict, str]:
    """
    Read S3 section of config.ini file and return dictionary of S3 credentials

    Args:
        conf (ConfigParser): ConfigParser object

    Returns:
        s3 (dict): dictionary of S3 credentials
        bucket (str): name of S3 bucket
    """
    conf: ConfigParser = get_conf()
    s3 = dict(conf["S3"])
    bucket: str = s3.pop("bucket")
    return s3, bucket
