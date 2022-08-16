import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from fastavro import schemaless_reader, schemaless_writer, parse_schema
from datetime import datetime
from google.cloud import storage
import json

'''
before deploying:
    create bucket for template 
    save schema avro file in bucket /schema/stream.avsc
'''
#ideally better to read from secure loation eg. cloud secret manager
PROJECT = "<PROJECT-NAME>"
DATASET = "events"
TABLE = "eth_swaps"
TOPIC_PATH = f"projects/{PROJECT}/topics/<TOPIC-NAME>"
BUCKET = "<BUCKET_NAME>"

def make_schema():
    mapping = {
        "usdc_amount": "FLOAT",
        "eth_amount": "FLOAT",
        "wallet": "STRING",
        "block": "INTEGER",
        "txn_hash": "STRING",
        "eth_price": "FLOAT",
        "BuySell": "STRING",
        "DateTime": "STRING",
        "WhaleAlert" : "BOOLEAN"
    }
    mapping_list = [{"mode": "NULLABLE", "name": k, "type": mapping[k]} for k in mapping.keys()]
    return {"fields": mapping_list}

#decoding and encoding
class HandleAvro:
    def __init__(self, schema):
        self.schema = schema
    
    def decode(self, element):
        from io import BytesIO
        fo = BytesIO(element)
        dict_record = schemaless_reader(fo, self.schema)
        return dict_record
    
    #not used here but can be used if saving records to cloud storage
    def encode(self, element):
        from io import BytesIO
        fo = BytesIO()
        schemaless_writer(fo, self.schema, element)
        bytes_array = fo.getvalue()
        return bytes_array

#add conditional fields
class BuySell(beam.DoFn):
    def process(self, element):
        element['BuySell'] = 'Buy' if element['usdc_amount'] < 0 else 'Sell'
        element['WhaleAlert'] = True if abs(element['usdc_amount']) > 5000000 else False
        return [element]

class AddTimeStamp(beam.DoFn):
    def process(self, element):
        element['DateTime'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        print(element)
        return [element]

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        #path to schema to handle avro as input
        parser.add_argument(
            '--schema',
            help="Avro schema location",
            default= f'{BUCKET}/schema/stream.avsc')


def run(input_topic, pipeline_args=None):
    #setting deployment arguments here instead of in shell command
    p_options = {
        'project': PROJECT,
        'runner': 'DataflowRunner',
        'staging_location': f'gs://{BUCKET}/staging',
        'temp_location': f'gs://{BUCKET}/temp',
        'template_location': f'gs://{BUCKET}/templates/transform',
        'region': 'us-central1',
        'setup_file': './setup.py',
        'save_main_session': True,
        'streaming': True
    }
    
    pipeline_options = PipelineOptions(
        pipeline_args, **p_options
    )
    my_options = pipeline_options.view_as(MyOptions)

    #read schema saved in cloud storage bucket
    schema_path = my_options.schema.split('/', 1)

    client = storage.Client()
    bucket = client.get_bucket(schema_path[0])
    blob = bucket.get_blob(schema_path[1])
    schema = json.loads(blob.download_as_string())
    avro_schema = parse_schema(schema)
    avro_handle = HandleAvro(avro_schema)

    table_schema = make_schema()

    with beam.Pipeline(options=pipeline_options) as pipeline:
        data =(
         pipeline | "Read from pubsub" >> beam.io.ReadFromPubSub(topic=input_topic)
        | "Decode Avro" >> beam.Map(lambda x: avro_handle.decode(x))
        | "Add Buy/Sell Label" >> beam.ParDo(BuySell())
        | "Add time stamp" >> beam.ParDo(AddTimeStamp())
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=TABLE,
            dataset = DATASET,
            project = PROJECT,
            schema = table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        result = pipeline.run()
        result.wait_until_finish()
        result.drain()

if __name__ == "__main__":
    run(TOPIC_PATH)