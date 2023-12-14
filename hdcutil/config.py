from __future__ import print_function, division, absolute_import
from configparser import ConfigParser
import os
from requests import request, Request, Response


__current_config: ConfigParser = None


def get_conf(filepath: str = "config.ini", force: bool = False) -> ConfigParser:
    """
    Read config.ini file and return ConfigParser object

    Args:
        filepath (str): path to config.ini file (default: "./config.ini")
        force (bool): force to read config.ini file (default: False)

    Returns:
        conf (ConfigParser): ConfigParser object
    """
    global __current_config
    if force == False and __current_config is not None:
        return __current_config

    filepath = os.environ.get("CONFIG_FILE", filepath)
    if os.path.exists(filepath):
        conf = ConfigParser()
        conf.read(filepath)
        __current_config = conf
        return conf
    elif os.environ.get("CONFIG_URL"):
        return get_conf_url(os.environ.get("CONFIG_URL"))

    raise FileNotFoundError("config.ini file is not found")


def get_conf_url(url: str) -> ConfigParser:
    """
    Read config.ini file from URL and return ConfigParser object

    Args:
        url (str): URL of config.ini file

    Returns:
        conf (ConfigParser): ConfigParser object
    """
    global __current_config
    r: Response = request("GET", url, timeout=5)
    if r.status_code == 200:
        conf = ConfigParser()
        conf.read_string(r.text)
        __current_config = conf
        return conf

    raise FileNotFoundError(f"url[{url}] is not found")


def conf_s3(section: str = "s3") -> dict:
    """
    Read S3 section of config.ini file and return dictionary of S3 credentials

    Args:
        conf (ConfigParser): ConfigParser object

    Returns:
        s3 (dict): dictionary of S3 Config credentials
    """
    conf: ConfigParser = get_conf()
    return dict(
        anon=conf.getboolean(section, "anon"),
        key=conf.get(section, "key"),
        secret=conf.get(section, "secret"),
        endpoint_url=conf.get(section, "endpoint_url"),
    )
