from flask import Flask, request
import threading
import requests
import os.path
import boto3
import flask
import time
import json
import os


app = Flask(__name__)
UPLOAD_DIRECTORY = "Blocks/"
INODE = 'inode'

#Get my DNS
response = requests.get('http://169.254.169.254/latest/meta-data/public-hostname')
CONST_DNS = response.text 
print(CONST_DNS)

#CHECK WHAT DAVE WANTS FOR THIS LATER~!
CONST_INTERVAL = 15

#Init sqs resource
sqs = boto3.resource('sqs')

heartbeat_url = 'https://sqs.us-west-2.amazonaws.com/494640831729/heartbeat'
heartbeat_queue = sqs.Queue(heartbeat_url)

#Recieve blocks and save them to local folder
@app.route('/blocks/', methods=['PUT'])
def writeBlock():
    #Check for local folder
    if not os.path.exists(UPLOAD_DIRECTORY):
        os.makedirs(UPLOAD_DIRECTORY)
  
    #Get the filename
    fileName = request.values['fileName']
    #Get number of blocks sent
    numBlocks = request.values['numBlocks']

    #Split the filename at the file extension
    splitName = fileName.split('.', 1)
    
    #Check for each block and save them if they exist
    i = 0
    while i < int(numBlocks):
        i += 1
        f = request.files[splitName[0] + '_' + str(i) + '.' + splitName[1]]
        #Save block to upload dir with block name
        f.save(os.path.join(UPLOAD_DIRECTORY, splitName[0] + '_' + str(i) + '.' + splitName[1]))

    print("Wrote new blocks")
    
    #Update Inode with new block data
    blocks = []
    for file in os.listdir(UPLOAD_DIRECTORY):
        blocks.append(file)
    inode = {"id": os.getenv('DATANODE_ID'), "blocks": blocks}
    open(INODE, "w+").write(json.dumps(inode))
    heartbeat_queue.send_message(MessageBody = open("inode", "r").read())
    
    blockList = getBlockNames(fileName)
    print(blockList)
    
    #forwardBlocks(blockList, 1)
    
    # Return 201 CREATED
    return "", 201

#Orders datanodes by DNS and scps file blocks to the next n nodes in the list
def forwardBlocks(blockList, n):
    response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
    my_id = response.text
   
    i = 0
    
    addrList = getDatanodeAddressList()
    
    
    for block in blockList:
        if i < n:
            print(my_id)

#Send heartbeats,
def sendHeartbeat():
    #Check for inode
    if not os.path.exists(INODE):
        open("inode", "w+").write({'id' : CONST_DNS})
    
    #Send a single heartbeat
    while(True):
        heartbeat_queue.send_message(MessageBody = open("inode", "r").read())
        time.sleep(CONST_INTERVAL - time.time() % CONST_INTERVAL)

def getBlockNames(fileName):
    blockList = []
    
    #split filename into name and extension
    nameArray = fileName.rsplit('.', 1)    

    i = 1
    while(os.path.exists(UPLOAD_DIRECTORY + nameArray[0] + "_" + str(i) + "." + nameArray[1])):
        blockList.append(UPLOAD_DIRECTORY + nameArray[0] + "_" + str(i) + "." + nameArray[1])
        i +=1
    
    return blockList

@app.route('/blocks/<blockname>', methods=['GET'])
def readBlock(blockname):
    return open(blockname, "r"), 200

@app.route('/blocks/<blockname>', methods=['POST'])
def replicateBlock(blockname):
    print("replicateBlock")


#Returns a list of dns addresses for all running datanodes
def getDatanodeAddressList():
    addressList = []

    #Filter for running instances
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    
    for instance in instances:
        if instance.tags != None:
            for tag in instance.tags:
                if ((tag["Key"] == 'Name') and (tag['Value'] == 'Datanode')):
                    addressList.append(instance.public_dns_name)
    return addressList

if __name__ == "__main__":
    #Start constant heartbeat on separate thread
    thread = threading.Thread(target=sendHeartbeat)
    thread.start()
    #Listen on ports
    app.run(debug=True, host="0.0.0.0", port=8000)