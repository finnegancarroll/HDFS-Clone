from flask import Flask
import boto3
import time
import json
import threading

app = Flask(__name__)
sqs = boto3.resource('sqs')
heartbeat_url = 'https://sqs.us-west-2.amazonaws.com/494640831729/heartbeat'
heartbeat_queue = sqs.Queue(heartbeat_url)

def sendHeartbeat():
    while(True):
        heartbeat_queue.send_message(
            MessageBody = open("inode", "r").read())
        time.sleep(20 - time.time() % 20)

thread = threading.Thread(target=sendHeartbeat)
thread.start()

@app.route('/blocks/<blockname>', methods=['GET'])
def readBlock(blockname):
    return open(blockname, "r"), 200

@app.route('/blocks/', methods=['PUT'])
def writeBlock():
    print("writeBlock")

@app.route('/blocks/<blockname>', methods=['POST'])
def replicateBlock(blockname):
    print("replicateBlock")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
