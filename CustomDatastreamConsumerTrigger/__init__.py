import logging
import azure.functions as func
from utils.utils import match_data_dictionary
import requests
from utils.config import SCHEMA_API,METADATA_API
import json
import logging
import copy
import traceback
from utils.base_schema import base_schema
from utils.postgresdb import generate_table_class, loadSession
from sqlalchemy.sql import exists 
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select,update
from dateutil.parser import parse
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from utils.config import AZURE_BLOB

logger = logging.getLogger(__name__)
session = loadSession()

def main(event: func.EventHubEvent):
   
    for e in event:
      try:
          logging.info('Python EventHub trigger processed a datastream: %s',
                 e.get_body().decode('utf-8'))
          logging.info(e.metadata)
          current_event = json.loads(e.get_body().decode('utf-8'))
          stream_type = current_event['streamName']
          logging.info("stream type")
          logging.info(stream_type)
          # match the stream name to the data dictionary
          stream_information = match_data_dictionary(stream_type)
          data_dict_params = {"data_type": "datastream"}
          data_dict_response = requests.post(SCHEMA_API['VALIDATE_DATA_PACKET'], 
          json=current_event, params=data_dict_params)
          params = {"user_id": current_event['individual_id'],"request_origin": f"{METADATA_API['REQUEST_ORIGIN']}","data_type": stream_type }
          headers = {
            "Authorization": f"{METADATA_API['TOKEN']}"
          }
          # 
          #
          # response = requests.get("https://api.personicle.org/data/read/metadata/datastream",headers=headers,params=params)
          # logging.info(response)
          if not json.loads(data_dict_response.text).get("schema_check", False):
              logger.error(f"Invalid event: {current_event}")
              return

          table_name = stream_information['TableName']
          individual_id = current_event['individual_id']
          source=current_event['source']

          if source == "Personicle":
              logger.info("Personicle source")
          unit = current_event['unit']
          model_class = generate_table_class(table_name, copy.deepcopy(base_schema[stream_information['base_schema']]))
          model_class_user_datastreams = generate_table_class("user_datastreams", copy.deepcopy(base_schema['user_datastreams_store.avsc']))

          data_stream_exists = session.query(exists().where( (model_class_user_datastreams.individual_id==individual_id) & 
          (model_class_user_datastreams.datastream == stream_type) & (model_class_user_datastreams.source == source))).scalar()

          confidence = current_event.get('confidence', None)
          record_values = []
          is_interval = "datastreams.interval" in stream_type

          for datapoint in current_event['dataPoints']:
              try:
                value = datapoint['value']
                # model_class = getClass(table_name) 
                if is_interval:
                  # logger.info("Interval data stream {}".format(stream_type))
                  start_time = datapoint['start_time']
                  end_time = datapoint['end_time']
                  # logger.info(f"Adding data point: individual_id: {individual_id} \n \
                  #   start_time= {start_time}, end_time={end_time}, source= {source}, value={value}, unit={unit}, confidence={confidence}")
                  record_values.append({"individual_id": individual_id,"start_time": start_time, "end_time": end_time,"source": source,"value": value,"unit": unit,"confidence": confidence})
                else:
                  # logger.info("Instantaneous data stream {}".format(stream_type))
                  timestamp = datapoint['timestamp']
                  # logger.info(f"Adding data point: individual_id: {individual_id} \n \
                  #   timestamp= {timestamp}, source= {source}, value={value}, unit={unit}, confidence={confidence}")
                  record_values.append({"individual_id": individual_id,"timestamp": timestamp,"source": source,"value": value,"unit": unit,"confidence": confidence})
              except Exception as e:
                logger.error("Error while adding point for datastream {}".format(stream_type))
                logger.error(e)
                continue

          if is_interval:
            max_timestamp = max(record_values, key=lambda dp: parse(dp['end_time']) )['end_time'] 
            min_timestamp = min(record_values, key=lambda dp: parse(dp['start_time']) )['start_time'] 
          else:
            max_timestamp = max(record_values, key=lambda dp: parse(dp['timestamp']) )['timestamp'] 
            min_timestamp = min(record_values, key=lambda dp: parse(dp['timestamp']) )['timestamp'] 

          if data_stream_exists:
              query = update(model_class_user_datastreams.__table__).where((model_class_user_datastreams.individual_id==individual_id) & 
              (model_class_user_datastreams.datastream == stream_type) & (model_class_user_datastreams.source == source)).values(last_updated = max_timestamp, table_name=table_name)
          else: 
              query = insert(model_class_user_datastreams.__table__).values(individual_id=individual_id,datastream=stream_type,last_updated=max_timestamp,source=source, table_name=table_name)

          session.execute(query)
          session.commit()
          
          statement = insert(model_class).values(record_values)

          if is_interval:
            statement = statement.on_conflict_do_nothing(index_elements=[model_class.individual_id, model_class.start_time, model_class.end_time, model_class.source])\
              .returning(model_class) 
            orm_stmt = (
                select(model_class)
                .from_statement(statement)
                .execution_options(populate_existing=True)
            )
          else:
            statement = statement.on_conflict_do_nothing(index_elements=[model_class.individual_id, model_class.timestamp, model_class.source])\
                .returning(model_class)
            orm_stmt = (
                select(model_class)
                .from_statement(statement)
                .execution_options(populate_existing=True)
            )

          return_values = session.execute(orm_stmt,)
          session.commit()
          # logger.info("inserted {} rows".format(len(list(return_values))))
          # print("inserted {} rows".format(len(list(return_values))))

          # if not source.startswith("Personicle"):
          #     #call data cleaning api
          #     logger.info("Calling data sync api")
          #     params = {"user": individual_id, "freq": "1min", "data": stream_type,"source":source,"starttime": min_timestamp, "endtime": max_timestamp}
          #     # res = requests.get(data_sync_api["ENDPOINT"], params=params).json()
          #     # logger.info(f"Datastream cleaned response: {res}")
          # checkpoint_store = BlobCheckpointStore.from_connection_string("{checkpoint}".format(checkpoint=AZURE_BLOB["CHECKPOINT"]), 
          #                   "{container}".format(container=AZURE_BLOB["CONTAINER"]))
      except Exception as e:
          session.rollback()
          logger.error(e)
          logger.error(traceback.format_exc())

