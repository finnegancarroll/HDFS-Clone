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


##TESTING VARIABLES, CHANGE FOR DEMO#####################
#CHECK WHAT DAVE WANTS FOR THIS LATER~!
CONST_INTERVAL = 15
#REP FAC 1 FOR TESTING ONLY, CHANGE BEFORE DEMO!!!
CONST_REP_FAC = 1

UPLOAD_DIRECTORY = "Blocks/"
CONST_PEM_KEY = "cpsc4910_1.pem"
CONST_REMOTE_UPLOAD = "/home/ec2-user/Team7/Datanode/Blocks"
INODE = 'inode'

#Get my DNS
response = requests.get('http://169.254.169.254/latest/meta-data/public-hostname')
CONST_DNS = response.text 

#Init sqs and ec2 recourse
sqs = boto3.resource('sqs')
ec2 = boto3.resource('ec2')

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
    
    #update inode and send post write heartbeat
    updateInode()
    heartbeat_queue.send_message(MessageBody = open("inode", "r").read())

    blockList = getBlockNames(fileName)
    
    forwardBlocks(blockList, CONST_REP_FAC)
    
    # Return 201 CREATED
    return "", 201

#Update Inode with new block data
def updateInode():
    blocks = []
    #Get block list
    for file in os.listdir(UPLOAD_DIRECTORY):
        blocks.append(file)
    inode = {"id": CONST_DNS, "blocks": blocks}
    #Write to inode
    open(INODE, "w+").write(json.dumps(inode))

#Orders datanodes by DNS and scps file blocks to the next n nodes in the list that are not me
def forwardBlocks(blockList, n):
    addrList = getDatanodeAddressList()
    
    #Blocks to replicate on new node
    sendFiles = ""
    for block in blockList:
        sendFiles += " " + block
    print(sendFiles)
   
    i = 0
    #Loop through nodes
    for address in addrList:
        #Check if the node is not me and I still need to make replicas 
        if address != CONST_DNS and i < n:
            print(sendToInstance(CONST_PEM_KEY, sendFiles, address, CONST_REMOTE_UPLOAD))
            i += 1

@app.route('/blocks/<blockname>', methods=['GET'])
def readBlock(blockname):
    return open(blockname, "r"), 200

@app.route('/blocks/<blockname>', methods=['POST'])
def replicateBlock(blockname):
    print("replicateBlock")

#Send files to another ec2 instance through scp with ssh key
def sendToInstance(keyPath, fileNames, instanceName, instancePath):
    cmd = "scp -i " + keyPath + " " + fileNames + " ec2-user@" + instanceName + ":" + instancePath
    print(cmd)
    return os.system(cmd)

#Send heartbeats,
def sendHeartbeat():
    #Check for inode
    if not os.path.exists(INODE):
        open("inode", "w+").write(json.dumps({'id' : CONST_DNS}))

    #Send heartbeats
    while(True):
        updateInode()
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

#Start constant heartbeat on separate thread
thread = threading.Thread(target=sendHeartbeat)
thread.start()

if __name__ == "__main__":
    #Listen on ports
    app.run(debug=True, host="0.0.0.0", port=8000)