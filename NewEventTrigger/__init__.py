import logging
import json
import azure.functions as func
from datetime import datetime
import copy
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, exists, inspect
import traceback

from utils.config import DB_CONFIG
from utils.postgresdb import generate_table_class, loadSession
from utils.base_schema import base_schema

logger = logging.getLogger(__name__)


def row2dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}


def main(event: func.EventHubEvent):
    session = loadSession()
    for e in event:
        logging.info('Python EventHub trigger processed an event: %s',
                     e.get_body().decode('utf-8'))

        event_body = json.loads(e.get_body().decode('utf-8'))
        print(event_body.keys())
        # Identify event type and name
        # dict_keys(['individual_id', 'start_time', 'end_time', 'event_name', 'source', 'parameters'])
        event_metadata = {
            "individual_id": event_body['individual_id'],
            "event_type": event_body['event_name'],
            "source": event_body['source'],
            "last_updated": datetime.utcnow(),
            "last_observed": event_body['end_time'],
            "observed_parameters": {},
            "total_occurences": 0
        }
        try:
            # get the row for the given event type and souce, if it exists and create the new row using the result

            table_name = DB_CONFIG['TABLENAME']
            logger.info("Adding to table {}".format(table_name))

            individual_id = event_body['individual_id']
            source = event_body['source']
            stream_type = event_body['event_name']

            # generate table class method also creates the table if it does not exist
            model_class = generate_table_class(table_name, copy.deepcopy(
                base_schema["metadata_schema"]))

            # run the following query, if the table or the row does not exist then return an initial row with 0 count
            select_query = select(model_class).where((model_class.individual_id == individual_id) &
                                                     (model_class.event_type == stream_type) & (model_class.source == source))
            result = session.execute(select_query).scalars().all()

            for row in result:
                logging.info("reading matched metadata row")
                logging.info(row2dict(row))

                data_packet = row2dict(row)

                data_packet['observed_parameters'] = json.loads(
                    data_packet['observed_parameters'])
                event_metadata = data_packet
                break

                # event_metadata["last_observed"] = event_body['end_time'],
            event_parameters = json.loads(event_body['parameters'])
            for k in event_parameters.keys():
                if type(event_parameters[k]) not in [int, float]:
                    continue

                event_metadata['observed_parameters'][k] = event_metadata['observed_parameters'].get(
                    k, 0) + 1
                # if k in event_metadata['observed_parameters'].keys():
                #     event_metadata['observed_parameters'][k] = event_metadata['observed_parameters']+1
                # else:
                #     event_metadata['observed_parameters'][k] = 1
            event_metadata['observed_parameters'] = json.dumps(
                event_metadata['observed_parameters'])

            event_metadata["total_occurences"] = event_metadata['total_occurences']+1
            # break

            logging.info(event_metadata)
            # insert the data in the generated model class

            statement = insert(model_class).values(event_metadata)
            statement = statement.on_conflict_do_update(index_elements=[model_class.individual_id, model_class.event_type, model_class.source], set_=event_metadata)\
                .returning(model_class.individual_id, model_class.event_type, model_class.source)
            orm_stmt = (
                select(model_class)
                .from_statement(statement)
                .execution_options(populate_existing=True)
            )

            print(statement)

            return_values = session.execute(orm_stmt,)
            for row in return_values.scalars():
                logger.info(row2dict(row))
            logger.info("inserted {} rows".format(len(list(return_values))))
            session.commit()
            session.close()

            # session.bulk_save_objects(objects)
        except Exception as e:
            session.rollback()
            session.close()
            logger.error(e)
            logger.error(traceback.format_exc())
            print(event_body)

    # Identify metadata to store (current time, event time stamp, parameters, source)
