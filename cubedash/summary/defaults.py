import os
from zoneinfo import ZoneInfo

DEFAULT_TTL = 90

# The default grouping epsg code to use on init of a new Explorer schema.
#
# We'll use a global equal area.
DEFAULT_EPSG = 6933

# Get default timezone via CUBEDASH_SETTINGS specified config file if it exists,
# otherwise default to Australia/Darwin
default_timezone = "Australia/Darwin"
settings_file = os.environ.get("CUBEDASH_SETTINGS", "settings.env.py")
try:
    with open(os.path.join(os.getcwd(), settings_file)) as config_file:
        for line in config_file:
            val = line.rstrip().split("=")
            if val[0] == "CUBEDASH_DEFAULT_TIMEZONE":
                default_timezone = val[1]
except FileNotFoundError:
    pass

DEFAULT_TIMEZONE = default_timezone
DEFAULT_GROUPING_TIMEZONE = ZoneInfo(DEFAULT_TIMEZONE)
