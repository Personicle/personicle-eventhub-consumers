from calendar import c
from typing import List
import logging
import json
import azure.functions as func
from utils.config import DB_CONFIG
from utils.postgresdb import generate_table_class, loadSession
from utils.base_schema import base_schema
import traceback
import copy
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)

def main(events: List[func.EventHubEvent]):
    session = loadSession()
    for event in events:
        logging.info('Python Events trigger processed an event: %s',
                        event.get_body().decode('utf-8'))
        

        try:
            current_event = json.loads(event.get_body().decode('utf-8'))
            stream_type = current_event['streamName']
            table_name =  table_name = DB_CONFIG['EVENTS_TABLENAME']
            individual_id = current_event['individual_id']
            source=current_event['source']
            stream_type = current_event['event_name']

            model_class = generate_table_class(table_name, copy.deepcopy(
            base_schema["user_events_schema"]))
            logger.info(current_event)
            # statement = insert(model_class).values(current_event)
        
        except Exception as e:
            session.rollback()
            session.close()
            logger.error(e)
            logger.error(traceback.format_exc())

    