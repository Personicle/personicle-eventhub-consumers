from asyncio.log import logger
import requests
from .config import SCHEMA_API
import logging
import json

logger = logging.getLogger(__name__)


def match_data_dictionary(stream_name):
    """
    Match a data type to the personicle data dictionary
    returns the data type information from the data dictionary
    """
    params = {'data_type': 'datastream', 'stream_name': stream_name}
    schema_response = requests.get(
        SCHEMA_API['MATCH_DICTIONARY_ENDPOINT']+"/match-data-dictionary", params=params)
    # data_stream = personcile_data_types_json["com.personicle"]["individual"]["datastreams"][stream_name]
    # return data_stream
    if not schema_response.status_code == 200:
        logger.warn(f"stream name {stream_name} not found")
    try:
        print(schema_response.text)
        print(schema_response.json())
        return schema_response.json()
    except Exception as e:
        print(e)
        return {"error": "No matching data stream"}


def validate(current_event):
    data_dict_params = {"data_type": "event"}
    data_dict_response = requests.post(SCHEMA_API['MATCH_DICTIONARY_ENDPOINT']+"/validate-data-packet",
                                       json=current_event, params=data_dict_params)

    if not json.loads(data_dict_response.text).get("schema_check", False):
        logger.error(f"Invalid event: {current_event}")
        return False
    return True
