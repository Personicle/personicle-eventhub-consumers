from configparser import ConfigParser
import os

if os.environ.get("ENVIRONMENT", "local") in ["local", "staging"]:
    config_object = ConfigParser()

    config_object.read("config.ini")

    SCHEMA_API = config_object['SCHEMA_API']
    DB_CONFIG = config_object['CREDENTIALS_DATABASE']
else:
    DB_CONFIG = {
        "USERNAME": os.environ.get("DB_USERNAME"),
        "PASSWORD": os.environ.get("DB_PASSWORD"),
        "HOST": os.environ.get("DB_HOST"),
        "NAME": os.environ.get("DB_DATABASE"),
        "TABLENAME": os.environ.get("DB_TABLENAME")
    }
