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

def check_json_keys(json_object, keys):
    if not all(key in json_object for key in keys):
        raise Exception("One or more keys are missing from the JSON object")

def check_null_values(json_object,non_null_keys):
     for key in non_null_keys:
         if json_object[key] is None or json_object[key] == '':
             raise Exception(f"The value of key '{key}' is null or empty.")
     

def main(events: List[func.EventHubEvent]):
    session = loadSession()
    for event in events:
        logging.info('Python Events trigger processed an event: %s',
                        event.get_body().decode('utf-8'))
        try:
            
            current_event = json.loads(event.get_body().decode('utf-8'))
            keys=["individual_id","start_time","end_time","event_name","source", "parameters"]

            check_json_keys(current_event,keys)
            check_null_values(current_event,keys)

            event_name = current_event['event_name']
            source = current_event['source']
            start_time=current_event['start_time']
            end_time=current_event['end_time']
            parameters=json.loads(current_event['parameters'])
            table_name =  table_name = DB_CONFIG['EVENTS_TABLENAME']
            individual_id = current_event['individual_id']
            source=current_event['source']

            model_class = generate_table_class(table_name, copy.deepcopy(
            base_schema["user_events_schema"]))
           
            query = insert(model_class.__table__).values(user_id=individual_id,start_time=start_time,end_time=end_time,event_name=event_name,parameters=parameters,source=source)
            # statement = insert(model_class).values(current_event)
            session.execute(query)
            session.commit()
        except Exception as e:
            session.rollback()
            session.close()
            logger.error(e)
            logger.error(traceback.format_exc())

    