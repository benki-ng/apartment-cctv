import picamera
import datetime
import boto3
import threading
import os

#The time (in seconds) in which a new recording file should be created
splitTime = 1800

#Set up s3
bucketName = "REDACTED"
regionName = "REDACTED"
endpointUrl = "REDACTED"
accessKey = "REDACTED"
secretKey = "REDACTED"


s3 = boto3.client("s3", region_name=regionName, endpoint_url=endpointUrl, aws_access_key_id=accessKey, aws_secret_access_key=secretKey)


#Upload to s3 function
def uploader(fileName):
    with open(fileName, 'rb') as data:
        try:
            s3.upload_fileobj(data, bucketName, fileName)
        except:
            print("Error on upload, not deleting local file.")
            return False
    # File uploaded successfully so local file deleted
    os.remove(fileName)
    return True


with picamera.PiCamera() as camera:
    camera.led = False
    camera.resolution = (640, 480)

    #Start initial recording
    date_time = datetime.datetime.now()
    nameStamp = (date_time.strftime("%Y-%b-%d_%H:%M") + '.h264') 
    print(nameStamp)
    camera.start_recording(nameStamp)
    camera.wait_recording(splitTime)

    while True:
        #Start upload of previous file to s3 in a background thread
        uploadThread = threading.Thread(target=uploader, kwargs={"fileName": nameStamp})
        uploadThread.start()

        #Roll over into new file
        date_time = datetime.datetime.now()
        nameStamp = (date_time.strftime("%Y-%b-%d_%H:%M") + '.h264')
        print(nameStamp)
        camera.split_recording(nameStamp)
        camera.wait_recording(splitTime)

