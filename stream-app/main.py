from google.cloud import pubsub_v1, dataflow_v1beta3
import base64
import json
from concurrent import futures
from dotenv import load_dotenv
from web3 import Web3
import asyncio
import os
from io import BytesIO
from google.pubsub_v1.types import Encoding
from fastavro import schemaless_writer, schema
from flask import Flask, request
from googleapiclient.discovery import build
import requests


app = Flask(__name__)
load_dotenv()

config = json.loads(open('config.json', 'r').read())
poolConfig = json.loads(open('poolConfig.json', 'r').read())

api_url =os.environ.get('WEB3_API')
uniswap_address = config['UNISWAP_ADDRESS']
uniswap_abi = config['UNISWAP_ABI']
wethAddress = config['WETH_ADDRESS']
usdcAddress = config['USDC_ADDRESS']

pool_address = poolConfig["WETH_USDC_POOL_ADDRESS"]
pool_ABI = poolConfig["WETH_USDC_POOL_ABI"]
erc20_abi_url = 'https://unpkg.com/@uniswap/v2-core@1.0.1/build/IERC20.json'

PROJECT_ID = os.environ.get("PROJECT_ID")
PUBSUB_TOPIC = "swaps_binary"
DATAFLOW_TEMPLATE = os.environ.get("DATAFLOW_TEMPLATE_PATH")
location = "us-central1"
dataflow_job_id = ""

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
avsc_file = r"stream.avsc"

def parse_swap(e):
    sqrtPriceX96 = e["args"]["sqrtPriceX96"]
    #if avro schema is contains more than one possible type for a field, 
    #type must be specified in the value eg. {'int': 123}
    data = {
        "usdc_amount": round(e["args"]["amount0"] / 10**6,6),
        "eth_amount": round(e["args"]["amount1"] /10**18,6),
        "wallet": e["args"]["recipient"],
        "block": e["blockNumber"],
        "txn_hash": e["transactionHash"].hex(),
        "eth_price": round(2 ** 192 / sqrtPriceX96 ** 2 * 10**12,6)
    }
    return data

def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = publish_future.result()
    print(message_id)

def handle_event(e):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
    #load avro schema
    avro_schema = schema.load_schema(avsc_file)
    fo = BytesIO()

    record = parse_swap(e)

    try:
        #get topic encoding type and decode
        topic = publisher.get_topic(request={'topic': topic_path})
        encoding = topic.schema_settings.encoding

        #handling binary and json encoding types
        if encoding == Encoding.BINARY:
            schemaless_writer(fo, avro_schema, record)
            data_publish = fo.getvalue()
        elif encoding == Encoding.JSON:
            data_publish = json.dumps(record).encode("utf-8")
        else:
            print(f"No encoding specified in {topic_path}. Abort.")
            exit(0)
        #publish to pubsub
        future = publisher.publish(topic_path, data=data_publish)
        print(data_publish)
        future.add_done_callback(callback)
        futures.wait([future], return_when=futures.ALL_COMPLETED)
        return future.result()
    except:
        print("could not publish")

#create and launch dataflow job from template
def launch_df():
    global dataflow_job_id
    try:
        dataflow = build('dataflow', 'v1b3')
        df_request = dataflow.projects().locations().templates().launch(
            projectId=PROJECT_ID,
            location = location,
            gcsPath = DATAFLOW_TEMPLATE,
            body={
                'jobName': 'stream_swap'
            }
        )
        response = df_request.execute()
        dataflow_job_id = response['job']['id']
        print(f'Dataflow job created with ID: {dataflow_job_id}')
                
    except:
        print("could not launch dataflow job")

#stop dataflow job
def drain_df():
    try:
        dataflow = build('dataflow', 'v1b3')
        df_request = dataflow.projects().locations().jobs().update(
            projectId=PROJECT_ID,
            location = location,
            jobId = dataflow_job_id,
            body={
                "requestedState": "JOB_STATE_DRAINED"
                }
        )
        response = df_request.execute()
        print(response)
        print("Dataflow job draining...")
    except:
        print(f"stream stopped \nDATAFLOW JOB IS STILL ACTIVE. Drain or Cancel job at \nhttps://console.cloud.google.com/dataflow/jobs/{location}/{dataflow_job_id}?project={PROJECT_ID}")

#task to run in loop
async def event_loop(events, poll_interval):
    while True:
        for swap in events.get_new_entries():
            handle_event(swap)
        await asyncio.sleep(poll_interval)

#initialize global variable loop - global variable needed as stop request needs to refer to same loop object
loop = None

@app.route('/service', methods = ['POST'])
def main():
    data = request.get_json(force=True)
    global loop
    if data['status'] == 'start':
        launch_df()

        try:
            web3 = Web3(Web3.HTTPProvider(api_url))
            latest = web3.eth.blockNumber
            pool_contract = web3.eth.contract(address=pool_address, abi=pool_ABI)
            events = pool_contract.events.Swap.createFilter(fromBlock = latest)
            
            #create async loop - loop get saved to global variable
            loop = asyncio.new_event_loop()
            loop.create_task(event_loop(events, 2))
            asyncio.set_event_loop(loop)
            loop.run_forever()
            return "running..."
        except:
            return "unable to start loop"
    elif data['status'] == 'stop':
        loop.stop()
        drain_df()
        print("stopped")
        return "stream stopped"
    else:
        return "invalid request"
    

if __name__=="__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
            