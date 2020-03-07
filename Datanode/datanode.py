from flask import Flask, request
import requests
import os.path
import boto3
import flask
import os

app = Flask(__name__)
UPLOAD_DIRECTORY = "Blocks/"

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
        f = request.files[splitName[0] + '_' + str(i) + '.' + splitName[1]]1
        #Save block to upload dir with block name
        f.save(os.path.join(UPLOAD_DIRECTORY, splitName[0] + '_' + str(i) + '.' + splitName[1]))

    print("Wrote new blocks")
    
    
    #
    blockList = getBlockNames(fileName)
    print(blockList)
    forwardBlocks(blockList, 1)
    
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
    app.run(debug=True, host="0.0.0.0", port=8000)