__all__ = ["send_notification"]

import json
import requests
import os
import warnings

_URL = "https://api.pushover.net/1/messages.json"

_LAST_TITLE = [""]
_LAST_MESSAGE = [""]


def get_config():
    filename = os.path.expanduser(os.path.join("~", ".pushover.json"))

    with open(filename, "rt") as fh:
        config = json.load(fh)

    assert "api_token" in config
    assert "user_key" in config
    assert "device" in config

    return config


def send_notification(title, message, sound="magic"):
    # Do not send the same message X-times in a row!
    if title == _LAST_TITLE[0] and message == _LAST_MESSAGE[0]:
        warnings.warn("Will not send a duplicate message!")
        return

    _LAST_TITLE[0] = title
    _LAST_MESSAGE[0] = message

    try:
        config = get_config()
    except FileNotFoundError:
        warnings.warn("Failed to open the pushover config file. No "
                      "notification has been.")
        return

    try:
        r = requests.post(_URL, data={
            "token": config["api_token"],
            "user": config["user_key"],
            "device": config["device"],
            "message": message,
            "title": title,
            "sound": sound})
        if not r.ok:
            warnings.warn("Failed to send notification because: %s" % r.reason)
    except requests.exceptions.ConnectionError:
        warnings.warn("Failed to send notification - connection error. No "
                      "internet access?")
