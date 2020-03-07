import flask
from flask import Flask, request
import boto3
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
        f = request.files[splitName[0] + '_' + str(i) + '.' + splitName[1]]
        print(splitName[0] + '_' + str(i) + '.' +  splitName[1])
        #Save block to upload dir with block name
        f.save(os.path.join(UPLOAD_DIRECTORY, splitName[0] + '_' + str(i) + '.' + splitName[1]))
    
    print("Wrote new blocks")
    # Return 201 CREATED
    return "", 201

@app.route('/blocks/<blockname>', methods=['GET'])
def readBlock(blockname):
    return open(blockname, "r"), 200

@app.route('/blocks/<blockname>', methods=['POST'])
def replicateBlock(blockname):
    print("replicateBlock")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)