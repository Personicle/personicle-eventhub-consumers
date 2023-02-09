from configparser import ConfigParser
import os


DB_CONFIG = {
    "USERNAME": os.environ.get("DB_USERNAME"),
    "PASSWORD": os.environ.get("DB_PASSWORD"),
    "HOST": os.environ.get("DB_HOST"),
    "NAME": os.environ.get("DB_DATABASE"),
    "TABLENAME": os.environ.get("DB_TABLENAME"),
    "EVENTS_TABLENAME": os.environ.get("EVENTS_TABLENAME")
}

AZURE_BLOB = {
    "CHECKPOINT": os.environ.get("AZURE_BLOB_CHECKPOINT"),
    "CONTAINER": os.environ.get("AZURE_BLOB_CONTAINER")
}

EVENTHUB = {
    "DATASTREAM_EVENTHUB_NAME" : os.environ.get("DATASTREAM_EVENTHUB_NAME"),
    "CONNECTION_STRING": os.environ.get("DATASTREAM_HUB_CONNECTION_STRING")
}

SCHEMA_API = {
    "MATCH_DICTIONARY_ENDPOINT": os.environ.get("MATCH_DICTIONARY_ENDPOINT"),
    "VALIDATE_DATA_PACKET": os.environ.get("VALIDATE_DATA_PACKET")
}