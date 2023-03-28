import json
import urllib.parse
import logging
import os
import boto3
from pathlib import Path
from base64 import b64decode
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
global dt
dt = datetime.now()

s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("BUCKET_NAME")
TARGET_BUCKET = os.getenv("TARGET_BUCKET")
TEMP_DIR = os.getenv("TEMP_DIR")


class c:
    
    def a(self,event,context):
        
        print(" ------------------------------------- ")
        print(" ------------------------------------- ")
        print("Starting the File Decryption Process ..")
        global key
        print("Extracting the uploaded base64 encoded file name from the s3 Bucket ..")
        
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        response = s3.get_object(Bucket=bucket, Key=key)
        
        print("The base64 encoded File name is: " + key)
        print("The base64 encoded File content type is: " + response['ContentType'])
        
        return response['ContentType']
        return key
    

    def b(self,context):
        
        print(" ------------------------------------- ")
        
        s3_file_path = key
        local_temp_file = f"{TEMP_DIR}/{key}"
        
        s = str({local_temp_file})
        fname = Path(s).resolve().stem
        print("The base64 encoded File name without extension :",fname)
        
        local_temp_folder = f"{TEMP_DIR}"
        local_temp_unzip = f"{TEMP_DIR}/{fname}/"
        
        print("The base64 encoded File is recieved, starting the file download to lambda temp folder ..")
        
        os.makedirs(TEMP_DIR, exist_ok=True)
        with open(local_temp_file, "wb") as f:
            s3.download_fileobj(BUCKET_NAME, s3_file_path, f)
            
        print("The base64 encoded File " + key + " is downloaded to Lambda /tmp folder ..")
        
        print(" ------------------------------------- ")
        print(" Contents in the /tmp folder as as below ")
        
        print(os.system(f"ls -la {local_temp_folder}"))
        
        print(" ------------------------------------- ")
        
        encry_file_path = f"{TEMP_DIR}/{key}" 
        print ("Encry_file_path output :",encry_file_path)
        
        encry_file_fname = f"{TEMP_DIR}/{fname}" 
        print ("Encry_file_fname output :",encry_file_fname)
        
        with open(encry_file_path, 'rb') as encr_file:
            encr_file_r = encr_file.read()
            
            
        encrypted_file = encr_file_r
        
        print("Encrypted base64 data is : ",encr_file_r)
        
        global decrypted 
        
        decrypted = boto3.client('kms', region_name='eu-central-1').decrypt(CiphertextBlob=b64decode(encrypted_file))['Plaintext']
        
        print("Decrypted base64 data is : ",decrypted)
        
        print(" ------------------------------------- ")
        
        print("Decryption process is complete, now uploading the decrypted zip file to the target bucket ..")
        
            
        #with open(encry_file_fname,'wb') as file_decrypted:
        #    file_decrypted.write(decrypted)
        #    file_decrypted.close()
        #    s3.upload_file(encry_file_fname, TARGET_BUCKET, fname)
        
        try:
            with open(encry_file_fname,'wb') as file_decrypted:
                file_decrypted.write(decrypted)
                file_decrypted.close()
                s3.upload_file(encry_file_fname, TARGET_BUCKET, fname)
            
            print("[SUCCESS]", dt, "The decrypted file is uploaded to the target bucket ..")
            
        
        except botocore.exceptions.ClientError as errorStdOut:
            
            print("[ERROR]", dt, "The decrypted file is NOT uploaded to the target bucket ..")
            
        
        #print("The decrypted file is uploaded to the target bucket ..")
        
        print(" ------------------------------------- ")
        print(" ------------------------------------- ")
        

        return {"statusCode": 200}  
        
    
def lambda_handler(event, context):
    
    x = c()
    x.a(event,context)
    x.b(event)
    
