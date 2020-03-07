import flask
from flask import Flask, request
import boto3
import os

app = Flask(__name__)
UPLOAD_DIRECTORY = os.getcwd() + "\\Blocks\\"

#Recieve blocks and save them to local folder
@app.route('/blocks/', methods=['PUT'])
def writeBlock():
    #Check for local folder
    if not os.path.exists(UPLOAD_DIRECTORY):
        os.makedirs(UPLOAD_DIRECTORY)
  
    #Get the filename
    fileName = request.values['fileName']

    #Get the binary and write it to upload directory
    f = request.files['block_n']
    if f:
        f.save(os.path.join(UPLOAD_DIRECTORY, fileName))
        
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