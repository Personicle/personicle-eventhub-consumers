from configparser import ConfigParser
import os


DB_CONFIG = {
    "USERNAME": os.environ.get("DB_USERNAME"),
    "PASSWORD": os.environ.get("DB_PASSWORD"),
    "HOST": os.environ.get("DB_HOST"),
    "NAME": os.environ.get("DB_DATABASE"),
    "TABLENAME": os.environ.get("DB_TABLENAME")
}
