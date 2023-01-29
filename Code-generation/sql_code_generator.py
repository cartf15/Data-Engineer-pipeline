import os 
import avro.schema
import avro.io
from io import BytesIO
import json
import re
import sqlparse


"""
DOCUMENTATION:
This python code is build for create a Snowflake pipeline end to end to process data from stage and implement Change Data Capture scripts ready to be executed in Snowflake.
Objtects created: 
Snowflake Tables, Stage, Streams, DataStreams, Pipeline, task and Views.
"""

path = '/Users/camilorestrepo/Documents/temp/temp-sql-objects/python_automation_output'
stage_name = input('insert the staging name: ')
avro_message = {
    "type": "record",
    "name": "DisputeWon",
    "namespace": "com.missionlane.sonic.messages.mmp",
    "doc": "Mission Lane won the Dispute",
    "fields": [
        {
            "name": "eventId",
            "type": "string",
            "doc": "UUID of this Transfer Event"
        },
        {
            "name": "clientId",
            "type": "string",
            "doc": "UUID of the client that initiated the parent transfer. Used for filtration purposes"
        },
        {
            "name": "transferId",
            "type": "string",
            "doc": "UUID of the parent Transfer"
        },
        {
            "name": "occurredAt",
            "type": "string",
            "doc": "the time at which the event occurred in ISO 8601 format."
        },
        {
            "name": "metadata",
            "type": {
                "type": "map",
                "values": [
                    "string"
                ]
            },
            "default": {},
            "doc": "Client-provided metadata for this transfer"
        },
        {
            "name": "amount",
            "type": [
                "null",
                "com.missionlane.sonic.messages.mmp.models.Amount"
            ],
            "default": None,
            "doc": "SHOULD NEVER BE NULL! Transfer Amount."
        },
        {
            "name": "amountDisputed",
            "type": "com.missionlane.sonic.messages.mmp.models.Amount",
            "doc": "Amount disputed"
        },
        {
            "name": "disputeReasonCode",
            "type": "com.missionlane.sonic.messages.mmp.models.DisputeReasonCode",
            "doc": "Reason code"
        },
        {
            "name": "disputeDescription",
            "type": "string",
            "doc": "Dispute description"
        },
        {
            "name": "disputeNetworkCode",
            "type": [
                "null",
                "string"
            ],
            "doc": "Card network dispute code",
            "default": None
        }
    ]
}

def extract_file_names(avro_message):
    return re.findall(r'"name": "([^"]+)",', json.dumps([field for field in avro_message["fields"]]))
def extract_file_names_and_types(avro_message):
    return [(field["name"], field["type"]) for field in avro_message["fields"]]

def map_avro_to_sql_type(avro_type, name):
    return ("TIMESTAMP" if avro_type == "string" and name[-2:].upper() == "AT" else "VARIANT" if avro_type == "string" and name[-8:] == "METADATA" else "VARCHAR" if avro_type == "string" else "INTEGER" if avro_type == "int" else "FLOAT" if avro_type == "float" else "DOUBLE" if avro_type == "double" else "BOOLEAN" if avro_type == "boolean" else "TIMESTAMP_NTZ(9)" if avro_type == "timestamp-micros" else "NUMBER")

def generate_sql_schema(avro_message):
    return ',\n'.join([f"{name} {map_avro_to_sql_type(type_,name)}" for name, type_ in extract_file_names_and_types(avro_message)]).upper()

def extract_name_and_transform(avro_message):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', ('mmp' + avro_message["name"])).lower(), ('mmp' + avro_message["name"]).lower()

def generate_task_schema(avro_message, message_name_snake_case):
    return ',\n'.join([f"stream.{message_name_snake_case}record:{name}::{'timestamp' if 'At' in name[-2:] else 'variant' if 'metadata' == name else map_avro_to_sql_type(type,name).lower()} as {name.split('::')[0]}" for name, type_ in extract_file_names_and_types(avro_message) if "At" in name[-2:] or "metadata" == name or name])

##CREATE VIEW
def create_view(message_name_snake_case,formated_fields):
    file_name=message_name_snake_case
    view_script = sqlparse.format(f"""
CREATE OR REPLACE VIEW PUBLIC.{message_name_snake_case.upper()} 
(
{formated_fields}
---standard fields
PUBSUB_MESSAGE_ID,
PUBSUB_PUBLISHED_EPOCH,
ETL_UPDATE_TIMESTAMP_UTC,
ETL_INSERT_TIMESTAMP_UTC
) AS 

SELECT
{formated_fields}
---standard fields
PUBSUB_MESSAGE_ID,
PUBSUB_PUBLISHED_EPOCH,
ETL_UPDATE_TIMESTAMP_UTC,
ETL_INSERT_TIMESTAMP_UTC
FROM SRC_ML_TB.{message_name_snake_case.upper()} ;

GRANT SELECT ON PUBLIC.{message_name_snake_case}  TO ROLE USER_SRC_ML_ROLE;
    """,icomma_first = True, ndent_tabs = True)
    return view_script, file_name

##CREATE TABLE
def create_table(message_name_snake_case,formated_fields_sql):
    file_name=message_name_snake_case
    table_script = sqlparse.format(f"""
CREATE TABLE IF NOT EXISTS SRC_ML_TB.{message_name_snake_case.upper()} 
(
{formated_fields_sql}
-- STANDART FIELDS 
PUBSUB_MESSAGE_ID NUMBER,
PUBSUB_PUBLISHED_EPOCH  NUMBER,
ETL_UPDATE_TIMESTAMP_UTC TIMESTAMP_NTZ(9),
ETL_INSERT_TIMESTAMP_UTC TIMESTAMP_NTZ(9)
); 

GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE ON TABLE SRC_ML_TB.{message_name_snake_case.upper()} TO ROLE ETL_BATCH_ROLE;
    """,icomma_first = True, ndent_tabs = True)
    return table_script, file_name

##CREATE DATASTREAM
def create_datastream(message_name_snake_case):
    file_name=message_name_snake_case+'_datastream'
    datastream_script = sqlparse.format(f"""
CREATE TABLE IF NOT EXISTS SRC_ML_TB.{file_name.upper()} 
(
{message_name_snake_case.upper()}_RECORD            VARIANT NOT NULL,
ETL_INSERT_TIMESTAMP_UTC        TIMESTAMP_NTZ NOT NULL DEFAULT LOCALTIMESTAMP COMMENT 'The UTC timestamp when record was inserted'
);

GRANT INSERT, SELECT, UPDATE, DELETE, TRUNCATE ON TABLE SRC_ML_TB.{file_name.upper()} TO ROLE ETL_BATCH_ROLE;
    """,icomma_first = True, ndent_tabs = True)
    return datastream_script, file_name

##PIPELINE
def create_pipeline(message_name_snake_case, message_lower_name):
    file_name=message_name_snake_case+'_pipe'
    notification = "{{notification}}"
    pipeline_script = sqlparse.format(f"""
CREATE PIPE IF NOT EXISTS SRC_ML_TB.{message_name_snake_case.upper()}_PIPE
AUTO_INGEST=TRUE
INTEGRATION= {notification}
AS
COPY INTO SRC_ML_TB.{message_name_snake_case.upper()}_DATASTREAM ({message_name_snake_case.upper()}_RECORD) 
FROM @SRC_ML_TB.{stage_name} pattern='.*/{message_lower_name}.*';
    """,icomma_first = True, ndent_tabs = True)
    return pipeline_script, file_name

##STREAM
def create_stream(message_name_snake_case):
    file_name=message_name_snake_case+'_stream'
    stream_script = sqlparse.format(f"""
CREATE STREAM IF NOT EXISTS SRC_ML_TB.{message_name_snake_case.upper()}_STREAM 
ON TABLE SRC_ML_TB.{message_name_snake_case.upper()}_DATASTREAM;
    """,icomma_first = True, ndent_tabs = True)
    return stream_script, file_name

##TASK
def create_task(message_name_snake_case,formated_fields,task_formated_fields):
    file_name= message_name_snake_case + '_task'
    task_script = f"""
CREATE TASK IF NOT EXISTS SRC_ML_TB.{message_name_snake_case.upper()}_TASK 
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
SCHEDULE = '5 minute'
WHEN SYSTEM$STREAM_HAS_DATA('SRC_ML_TB.{message_name_snake_case.upper()}_STREAM')
AS
INSERT INTO SRC_ML_TB.{message_name_snake_case.upper()} (
{formated_fields}
---standard fields
PUBSUB_MESSAGE_ID,
PUBSUB_PUBLISHED_EPOCH,
ETL_UPDATE_TIMESTAMP_UTC,
ETL_INSERT_TIMESTAMP_UTC
)
WITH streamed_data AS (
SELECT

    current_stream.{message_name_snake_case.upper()}_RECORD:pubsubMessageId::varchar AS pubsubMessageId 
    ,current_stream.{message_name_snake_case.upper()}_RECORD
    , row_number () over (
        partition by {message_name_snake_case.upper()}_RECORD:pubsubMessageId::varchar 
        order by {message_name_snake_case.upper()}_RECORD:pubsubMessageId::varchar
    ) as row_number
FROM src_ml_tb.{message_name_snake_case.upper()}_STREAM current_stream
LEFT JOIN src_ml_tb.{message_name_snake_case.upper()} target 
    ON target.pubsub_Message_Id = current_stream.{message_name_snake_case.upper()}_RECORD:pubsubMessageId::varchar
WHERE target.pubsub_Message_Id IS NULL -- Handle pubsub duplicates

)    


SELECT 
    {task_formated_fields}
    ------ standard fields
    stream.{message_name_snake_case.upper()}_record:pubsubMessageId::number as pubsub_message_id,
    stream.{message_name_snake_case.upper()}_record:pubsubPublished::number as pubsub_published_epoch,
    CURRENT_TIMESTAMP() AS etl_update_timestamp_utc,
    CURRENT_TIMESTAMP() AS etl_insert_timestamp_utc
    FROM streamed_data stream
    WHERE row_number = 1;

ALTER TASK SRC_ML_TB.{message_name_snake_case.upper()}_TASK RESUME;
    
    """
    return task_script, file_name

##CREATE FILES
def create_files(message_name_snake_case,path):
    ##view
    view_script, file_name = create_view(message_name_snake_case,formated_fields)
    with open(os.path.join(path+'/view', f'{file_name}.sql'), 'w') as f:
        f.write(view_script)
    print(f'file {file_name} executed correctly')
    ##table
    table_script, file_name = create_table(message_name_snake_case,formated_fields_sql)
    with open(os.path.join(path, f'{file_name}.sql'), 'w') as f:
        f.write(table_script)
    print(f'file {file_name} executed correctly')

    ##datastream
    datastream_script, file_name = create_datastream(message_name_snake_case)
    with open(os.path.join(path, f'{file_name}.sql'), 'w') as f:
        f.write(datastream_script)
    print(f'file {file_name} executed correctly')

    ## pipe 
    pipe_script, file_name = create_pipeline(message_name_snake_case, message_lower_name)
    with open(os.path.join(path, f'{file_name}.sql'), 'w') as f:
        f.write(pipe_script)
    print(f'file {file_name} executed correctly')

    # stream 
    stream_script, file_name = create_stream(message_name_snake_case)
    with open(os.path.join(path, f'{file_name}.sql'), 'w') as f:
        f.write(stream_script)
    print(f'file {file_name} executed correctly')

    task_script, file_name = create_task(message_name_snake_case,formated_fields,task_formated_fields)
    with open(os.path.join(path, f'{file_name}.sql'), 'w') as f:
        f.write(task_script)
    print(f'file {file_name} executed correctly')
    
    return print('all files created correctly')


def init_variables (avro_message):
        


    ##FILE NAME
    message_name_snake_case, message_lower_name = extract_name_and_transform(avro_message)
    #print(message_name_snake_case)

    #FIELDS 
    fields = extract_file_names(avro_message)
    formated_fields = ',\n'.join(fields).upper()
    formated_fields = formated_fields + ','
    #print(formated_fields)

    #FIELD DATA TYPE
    up_formated_fields = ',\n'.join(fields).upper()
    formated_fields_sql = generate_sql_schema(avro_message)
    formated_fields_sql = formated_fields_sql + ','
    #print(formated_fields_sql)

    #TASK FIELDS
    task_formated_fields = generate_task_schema(avro_message, message_name_snake_case)
    #print('\n',task_formated_fields)
    return message_name_snake_case, message_lower_name, formated_fields, up_formated_fields, formated_fields_sql, task_formated_fields


if __name__ == "__main__":
    try: 
        message_name_snake_case, message_lower_name, formated_fields, up_formated_fields, formated_fields_sql, task_formated_fields = init_variables(avro_message)
        create_files(message_name_snake_case,path)
    except Exception as e:  
        print("An error ocurred", e)
