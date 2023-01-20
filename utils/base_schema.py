from sqlalchemy import BigInteger, Column, Float, TIMESTAMP
from sqlalchemy.types import Integer, Numeric, String
from sqlalchemy.dialects.postgresql import JSON, UUID
import uuid
from datetime import datetime

base_schema = {
    "integer_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Integer),
        "unit": Column(String),
        "confidence": Column(String, default=None)
    },
    "numeric_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Float),
        "unit": Column(String),
        "confidence": Column(String, default=None)
    },
    "string_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(String),
        "unit": Column(String),
        "confidence": Column(String, default=None)
    },
    "event_schema.avsc": {
        "user_id": Column(String, primary_key=True),
        "start_time": Column(TIMESTAMP, primary_key=True),
        "end_time": Column(TIMESTAMP, primary_key=True),
        "event_name": Column(String, primary_key=True),
        "source": Column(String, primary_key=True),
        "parameters": Column(JSON),
        # , default=uuid.uuid4, unique=True)
        "unique_event_id": Column(UUID(as_uuid=False))
    },
    "metadata_schema": {
        "individual_id": Column(String, primary_key=True),
        "event_type": Column(String, primary_key=True),
        "source": Column(String, primary_key=True),
        "last_updated": Column(TIMESTAMP, default=datetime.utcnow()),
        "last_observed": Column(TIMESTAMP),
        "observed_parameters": Column(JSON),
        "total_occurences": Column(Numeric)
    }
}
