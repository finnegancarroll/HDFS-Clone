from flask import Flask, request
import os
import boto3
import time
import json
import threading

MSG_IN_RATE = 10

#Time it takes for a node to timeout in seconds
#THESE WILL NEED TO BE CHANGED BEFORE DEMO, UPDATE TIME IS SUPPOSED TO BE 30 AND TIMEOUT IS UP TO US??? DOUBLE CHECK TIMEOUT
CONST_SLEEP_INTERVAL = 20
CONST_SQS_POLL_RATE = 10
CONST_TIMEOUT = 2

app = Flask(__name__)
sqs = boto3.resource('sqs')
heartbeat_queue = sqs.Queue('https://sqs.us-west-2.amazonaws.com/494640831729/heartbeat')
#Dictionary of datanodes by key DNS name
datanodes_dict = {}
files_dict = {}

def check_queue():
    while(True):
        #Keys marked for deletion
        #Note: Must remove keys this way because you cannot change a dictionary while iterating over it
        delKeys = []
        
        #Get next set of messages in SQS
        messages = heartbeat_queue.receive_messages(MaxNumberOfMessages=MSG_IN_RATE, WaitTimeSeconds=CONST_SQS_POLL_RATE)
        
        #Consume messages
        #Note: 10 messages consumed at once, namenode is updated, and then status is printed
        for message in messages:
            msgDict = json.loads(message.body)
            parse_heartbeat_messages(msgDict)

            message.delete()
        
        #Print time since last heartbeat for every recorded non timedout datanode
        for key in datanodes_dict:
            if ((datanodes_dict[key])["updated"] == False):
                (datanodes_dict[key])["time_since_last_heartbeat"] += 1
            print("ID: " + key + " HEARTBEAT: " + str(datanodes_dict[key]["time_since_last_heartbeat"]))
            datanodes_dict[key]["updated"] = False
            #Check for node timeout
            if datanodes_dict[key]["time_since_last_heartbeat"] >= CONST_TIMEOUT:
                print("ID: " + key + " HAS TIMED OUT -> MARKING DEAD")
                #Remove dead node
                delKeys.append(key)
        
        #Actually remove the dead nodes from the dict
        for key in delKeys:
            del datanodes_dict[key]
        
        #Sleep till the next 
        time.sleep(CONST_SLEEP_INTERVAL - time.time() % CONST_SLEEP_INTERVAL)

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

@app.route('/files/', methods=['GET'])
def listFiles():
    return json.dumps(files_dict), "200"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)