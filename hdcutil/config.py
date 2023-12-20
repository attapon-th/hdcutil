from __future__ import print_function, division, absolute_import
from configparser import ConfigParser
import os
from requests import request, Response
from typing import Any


__current_config: ConfigParser | None = None
__hdc_schema: dict[str, Any] | None = None


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
    if force is False and __current_config is not None:
        return __current_config

    filepath = os.environ.get("CONFIG_FILE", filepath)
    if os.path.exists(filepath):
        conf = ConfigParser()
        conf.read(filepath)
        __current_config = conf
        return conf
    elif os.environ.get("CONFIG_URL"):
        return get_conf_url()

    raise FileNotFoundError("config.ini file is not found")


def get_conf_url() -> ConfigParser:
    """
    Read config.ini file from URL and return ConfigParser object

    Args:
        url (str): URL of config.ini file

    Returns:
        conf (ConfigParser): ConfigParser object
    """
    global __current_config
    url: str | None = os.environ.get("CONFIG_URL")

    r: Response = request(
        "GET",
        os.environ.get("CONFIG_URL", ""),
        timeout=5,
        allow_redirects=True,
        headers={
            "Authorization": "Basic {}".format(os.environ.get("CONFIG_BASIC_AUTH"))
        },
    )
    if r.status_code == 200:
        conf = ConfigParser()
        conf.read_string(r.text)
        __current_config = conf
        return conf

    raise FileNotFoundError(f"url[{url}] is not found")
