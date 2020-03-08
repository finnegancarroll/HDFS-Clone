#SUFS Client program

from fsplit.filesplit import FileSplit #pip3 install filesplit or pip2 install fsplit
from flask import request
import requests as req
from os import path
import os.path
import boto3
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
CONST_BLOCK_SIZE = .5

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
            print("create s3_file_name")
            print("read SUFS_file_name")
            print("list SUFS_file_name")
        elif inList[0]  == CONST_CREATE:
            createCMD(inList[1], bucket) #have to pass in object bucket here so its reachable
        elif inList[0] == CONST_READ:
            readCMD(inList[1])
        elif inList[0] == CONST_LIST:
            listCMD(inList[1])
        else:
            print("Inproper syntax")
            print("\"SUFS$ help\" for more options")   


#########COMMAND LINE FUNCTIONS STUBS#########

#$create "filePath" "buckName"
#Write specified file from s3 BUCKET into SUFS
def createCMD(fileName, bucket):
    
    #Download file from s3
    bucket.download_file(fileName, CONST_DOWN + fileName) 
    
    #Split file
    splitFile(fileName)
    
    #GET DESIGNATED DATANODE ADDRESS FROM THE NAMENODE!!!
    #CURENTLY HARDCODED FOR TESTING
    address = "ec2-52-36-160-84.us-west-2.compute.amazonaws.com"
    
    #Upload file blocks
    sendBlocks(address, getBlockNames(fileName), fileName)

#$list "filename"
#Retrieve the list of blocks associated with that file and the datanodes they live on 
def listCMD(filename):
    print(filename)
    
#$read "filename"
#Download file from SUFS onto LOCAL machine
def readCMD(filename):
    print(filename)

#########HELPER FUNCTIONS#########

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
    print(r.status_code)

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

#Create downloads folder if it does not exits
def initDirs():   
    if not os.path.exists(CONST_DOWN):
        os.makedirs(CONST_DOWN)
   
if __name__ == '__main__':
    main()