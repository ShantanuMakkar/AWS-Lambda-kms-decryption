import json
import urllib.parse
import logging
import os
import zipfile
import boto3
from io import BytesIO
from pathlib import Path
from base64 import b64decode


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("BUCKET_NAME")
TARGET_BUCKET = os.getenv("TARGET_BUCKET")
TEMP_DIR = os.getenv("TEMP_DIR")


class c:
    
    def a(self,event,context):
        
        print(" ------------------------------------- ")
        print("Starting the File transfer process ..")
        global key
        print("Extracting the uploaded file name from the s3 Bucket ..")
        
        #Extract Bucket and Object Name
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        response = s3.get_object(Bucket=bucket, Key=key)
        
        print("The uploaded File name is: " + key)
        print("The uploaded File content type is: " + response['ContentType'])
        
        return response['ContentType']
        return key
    

    def b(self,context):
        
        print(" ------------------------------------- ")
        print("File name recieved, starting the file download to lambda temp folder ..")
        
        #Defining Variables
        s3_file_path = key
        local_temp_file = f"{TEMP_DIR}/{key}"
        
        s = str({local_temp_file})
        fname = Path(s).resolve().stem
        print("File name without extension :",fname)
        
        local_temp_folder = f"{TEMP_DIR}"
        local_temp_unzip = f"{TEMP_DIR}/{fname}/"
        
        
        os.makedirs(TEMP_DIR, exist_ok=True)
        with open(local_temp_file, "wb") as f:
            s3.download_fileobj(BUCKET_NAME, s3_file_path, f)
            
        print("Zipped File " + key + " is downloaded to Lambda /tmp folder ..")
        
        print(" ------------------------------------- ")
            
        print(os.system(f"ls -la {local_temp_folder}"))
        
        print(" ------------------------------------- ")
        print(" ------------------------------------- ")
        
        
        with open('/tmp/kms-unzip-demo.zip.base64', 'rb') as u:
            h = u.read()
            
            
        encrypted_file = h
        
        print("Encrypted base64 data is : ",h)
        
        global decrypted 
        
        decrypted = boto3.client('kms', region_name='us-east-2').decrypt(CiphertextBlob=b64decode(encrypted_file))['Plaintext']
        
        g = str(decrypted)
        print("Decrypted base64 data is : ",g)
    
        print("Decrypted base64 data is : ",decrypted)
        
        
        
        #j = open(demoo, 'x')
        #j.write()
        
        with open("/tmp/kms-unzip-demo.zip",'wb') as file_decrypted:
            file_decrypted.write(decrypted)
            file_decrypted.close()
            #print(os.system(f"cat /tmp/demoo.txt"))
            #s3.upload_fileobj(file_decrypted, BUCKET_NAME, "demoo")
            s3.upload_file("/tmp/kms-unzip-demo.zip", TARGET_BUCKET, "kms-unzip-demo.zip")
            
        
        print(os.system(f"ls -la {local_temp_folder}"))
        

        return {"statusCode": 200}  
        
    
def lambda_handler(event, context):
    
    x = c()
    x.a(event,context)
    x.b(event)
    
