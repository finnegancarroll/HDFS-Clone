from flask import Flask, request
import os
import boto3
import time
import json
import threading

app = Flask(__name__)
sqs = boto3.resource('sqs')
heartbeat_queue = sqs.Queue('https://sqs.us-west-2.amazonaws.com/494640831729/heartbeat')
heartbeat_dict = {"time_since_last_heartbeat": 0, "updated": False} 
datanodes_dict = {}
files_dict = {}

def check_queue():
    while(True):
        messages = heartbeat_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=10)
        for message in messages:
            msgDict = json.loads(message.body)
            parse_heartbeat_messages(msgDict)
            message.delete()

        for key, value in datanodes_dict.items():
            if (value["updated"] == False):
                value["time_since_last_heartbeat"] += 1
            value["updated"] = False
            print("ID: " + key + " HEARTBEAT: " + value["time_since_last_heartbeat"])

        print(files_dict)
        time.sleep(10 - time.time() % 10)

def parse_heartbeat_messages(msgDict):
    msgId = msgDict["id"]

    for block in msgDict["blocks"]:
        blockNameArr = block.rsplit('.',1)
        fileName = blockNameArr[0].rsplit('_', 1)[0] + "." + blockNameArr[1]
        
        if (not files_dict.get(fileName)): files_dict[fileName] = {}
        if(not files_dict[fileName].get(blockNameArr[0])): files_dict[fileName][blockNameArr[0]] = []
        if(msgId not in files_dict[fileName][blockNameArr[0]]): files_dict[fileName][blockNameArr[0]].append(msgId)

    if (not datanodes_dict.get(msgId)):
        datanodes_dict[msgId] = heartbeat_dict
    
    datanodes_dict[msgId]["time_since_last_heartbeat"] = 0
    datanodes_dict[msgId]["updated"] = True
   
thread = threading.Thread(target=check_queue)
thread.start() 

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)