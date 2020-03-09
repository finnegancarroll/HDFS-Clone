from fsplit.filesplit import FileSplit
from flask import request
import requests as req
from os import path
import os.path
import boto3
import json
import sys
import os

#Download folder
CONST_DOWN = "Downloads/" 

#Console Interface
CONST_HELP = "help"
CONST_EXIT = "exit"
CONST_CREATE = "create"
CONST_READ = "read"
CONST_LIST = "list"

#Block size in MB
###########################DONT FORGET TO CHANGE THIS BACK!!!
CONST_BLOCK_SIZE = 0.5

#Bytes per MB
CONST_BYTES_PER_MB = 1000000

#Public port on datanode DNS
CONST_DATANODE_PORT = 8000

bucket = boto3.resource('s3').Bucket("termprojbucket")
ec2 = boto3.resource('ec2')

def main():

    initDirs()
    
    active = True
    while active:
        inputStr = input("SUFS$ ")
        inList = inputStr.split(' ')
        
        if inputStr == CONST_EXIT:
            active = False
        elif inputStr == CONST_HELP:
            print("usage:")
            print(" create s3_file_name")
            print(" read SUFS_file_name")
            print(" list SUFS_file_name\n")
        elif ((inList[0]  == CONST_CREATE) and (len(inList) == 2)):
            createCMD(inList[1], bucket) #have to pass in object bucket here so its reachable
        elif ((inList[0] == CONST_READ) and (len(inList) == 2)):
            readCMD(inList[1])
        elif ((inList[0] == CONST_LIST) and (len(inList) == 2)):
            listCMD(inList[1])
        elif inList[0] == "":
            doNothing = 0
        else:
            print("Inproper syntax")
            print("SUFS$ help for more options\n") 

#########COMMAND LINE FUNCTIONS STUBS#########

#$create "filePath" "buckName"
#Write specified file from s3 BUCKET into SUFS
def createCMD(fileName, bucket):
    
    #Download file from s3
    bucket.download_file(fileName, CONST_DOWN + fileName) 
    
    #Split file
    splitFile(fileName)
   
    #Get random datanode to put data on from the namenode
    temp_req = req.put("http://" + getNameNodeAddress() + ":8000/files").text
    namenode_req = "%s" %temp_req

    #Upload file blocks
    status_code = sendBlocks(namenode_req, getBlockNames(fileName), fileName)
    
    #Delete local blocks
    deleteBlocks(getBlockNames(fileName))

    print("Status Code: " + str(status_code))

#$list "filename"
#Prints file, blocks, and block locations for filename
def listCMD(filename):
    #Get request returns dictionary of all blocks and datanodes
    r = req.get("http://" + getNameNodeAddress() + ":8000/files")
    formatList(r.text, filename)
    print("Status Code: " + str(r.status_code))

#$read "filename"
#Download file from SUFS onto LOCAL machine
def readCMD(filename):
    #Splite the filename into name and file extension
    fileNameSplit = filename.split('.')

    #Get block num and datanode address from the namenode
    fileInfo = getFileInfo(filename)
    dataNodeDNS = fileInfo["dns"]
    totalBlocks = fileInfo["blocks"]
    
    print("Downloading Blocks")
    print("DATANODE DNS: " + dataNodeDNS)
    
    for i in range(0, totalBlocks):
        blockName = fileNameSplit[0] + '_' + str(i + 1) + '.' + fileNameSplit[1]
        r = req.get("http://" + dataNodeDNS + ":8000/blocks/" + blockName)
        
        #Save block to download dir with block name
        file = open(CONST_DOWN + blockName, "wb")
        file.write(r.content)
        file.close()
   
    blockList = getBlockNames(filename)
    
    #Merge the blocks back to original file
    print("Merging blocks...")
    mergeFile(blockList, filename)
    
    #Delete blocks
    deleteBlocks(blockList)
    
    print("Status Code: " + str(r.status_code))

#########HELPER FUNCTIONS#########

#Gets the number of blocks and datanode associated with a file on the SUFS
def getFileInfo(filename):
    r = req.get("http://" + getNameNodeAddress() + ":8000/blocks/" + filename).text
    reqToString = str(r)
    fixReqFormat = reqToString.replace("'", "\"")
    reqToJSON = json.loads(fixReqFormat)
    return reqToJSON

#Print the file/datanode/block string returned from namenode
def formatList(outList, fileName):
    #Delete grabage chars
    removeThese = {':' , '{', ']' , '[' , ':' , ',' , ' '}
    for char in removeThese:
        outList = outList.replace(char, '')
    
    #Tab DNS
    outList = outList.replace('ec2', '    ec2')

    #Devide the output by files
    outList = outList.split('}')
    
    #Find fileLine
    fileLine = ""
    for fileInfo in outList:
        lineHeader = "\"" + fileName + "\"" 
        if lineHeader  == fileInfo[:len(lineHeader)]:
            fileLine = fileInfo

    #Split into fields
    i = 0
    fileFields = fileLine.split('"')
    for field in fileFields:
        if i == 1:
            print("File Name: " + field)
        elif i == 2:
            print("\n---------Blocks---------\n" + field)
        else:
            print(field)
        i += 1

#Sends all blocks to datanode at address
def sendBlocks(address, blockList, fileName):
    files = {}

    #Add all file blocks
    i = 1
    for block in blockList:
        #Block looks like CONST_DOWN/blockName
        #But the key should just be blockName
        #So we split at '/' and take the second half
        splitBlockDir = block.split('/', 1)
        files[splitBlockDir[1]] = open(block,'rb')
    
    #Include filename
    values = {'fileName': fileName, 'numBlocks' : len(blockList)}
    
    #Send to designated Datanode
    url = "http://" + address + ":" + str(CONST_DATANODE_PORT) + "/blocks/"
    r = req.put(url, files=files, data=values)
    return r.status_code

#Splits a file in the downloads folder into blocks
#Original file deleted
def splitFile(fileName):
    fs = FileSplit(file=CONST_DOWN + fileName, splitsize=CONST_BLOCK_SIZE * CONST_BYTES_PER_MB, output_dir=(CONST_DOWN))
    fs.split()
    os.remove(CONST_DOWN + fileName)

#Merge all blocks of the file
#Merges in order of fileNames array and result is placed in Downloads folder
def mergeFile(blockNameArray, fileName):
    with open(CONST_DOWN + fileName, 'wb') as merged:
        for blockName in blockNameArray:
            with open(blockName, 'rb') as currBlock:
                merged.write(currBlock.read())            
    
#Deletes all the blocks listed in blocklist
def deleteBlocks(blockList):
    for blockPath in blockList:
        os.remove(blockPath)

#Returns all the blocks in the downloads folder associated with the designated file
def getBlockNames(fileName):
    blockList = []
    
    #split filename into name and extension
    nameArray = fileName.rsplit('.', 1)    

    i = 1
    while(path.exists(CONST_DOWN + nameArray[0] + "_" + str(i) + "." + nameArray[1])):
        blockList.append(CONST_DOWN + nameArray[0] + "_" + str(i) + "." + nameArray[1])
        i +=1
    
    return blockList

#Returns DNS for the current running namenode
#Function will not consider multiple namenode cases
def getNameNodeAddress():
    #Filter for running instances
    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    
    for instance in instances:
        if instance.tags != None:
            for tag in instance.tags:
                if ((tag["Key"] == 'Name') and (tag['Value'] == 'Namenode')):
                    return instance.public_dns_name

#Create downloads folder if it does not exits
def initDirs():   
    if not os.path.exists(CONST_DOWN):
        os.makedirs(CONST_DOWN)

if __name__ == '__main__':
    main()