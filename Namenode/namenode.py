from flask import Flask, request
import os
import boto3
import time
import json
import threading

app = Flask(__name__)
sqs = boto3.resource('sqs')
heartbeat_queue = sqs.Queue('https://sqs.us-west-2.amazonaws.com/494640831729/heartbeat')
datanodes_dict = {}
files_dict = {}

def check_queue():
    while(True):
        messages = heartbeat_queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=10)
        for message in messages:
            msgDict = json.loads(message.body)
            parse_heartbeat_messages(msgDict)

            message.delete()
        
        for key in datanodes_dict:
            if ((datanodes_dict[key])["updated"] == False):
                (datanodes_dict[key])["time_since_last_heartbeat"] += 1
            print("ID: " + key + " HEARTBEAT: " + str(datanodes_dict[key]["time_since_last_heartbeat"]))
            datanodes_dict[key]["updated"] = False      

        time.sleep(20 - time.time() % 20)

def parse_heartbeat_messages(msgDict):
    msgId = msgDict["id"]

    if ("blocks" in msgDict):
        for block in msgDict["blocks"]:
            blockNameArr = block.rsplit('.',1)
            fileName = blockNameArr[0].rsplit('_', 1)[0] + "." + blockNameArr[1]
            
            if (not files_dict.get(fileName)): files_dict[fileName] = {}
            if(not files_dict[fileName].get(blockNameArr[0])): files_dict[fileName][blockNameArr[0]] = []
            if(msgId not in files_dict[fileName][blockNameArr[0]]): files_dict[fileName][blockNameArr[0]].append(msgId)

    if (not datanodes_dict.get(msgId)):
        datanodes_dict[msgId] = {}
    
    datanodes_dict[msgId]["time_since_last_heartbeat"] = 0
    datanodes_dict[msgId]["updated"] = True
   
thread = threading.Thread(target=check_queue)
thread.start() 

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)