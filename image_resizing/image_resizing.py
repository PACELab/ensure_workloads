
#from scipy import misc
#import imageio

import json,subprocess
from PIL import Image
import boto
import boto.s3.connection


access_key = "TBD"
secret_key = "TBD" #os.getenv('AWS_SECRET_ID',"")
hostname = "TBD" 
portNum = "TBD"

def main(args):

    name = args.get("params", "stranger")
    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)

        bucketName = msg["bucketName"] 
        keyName = msg["keyName"] 
    else:
        bucketName = "images"
        keyName = "action_input"
        #return "Error: params not found!! name -->"+str(name)

    bucketStr = hostname
    conn = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = hostname,
            port = portNum,
            is_secure=False,               # uncomment if you are not using ssl
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )

    #bucketName = args.get("bucketName","images")
    #keyName = args.get("keyName","action_input")
    bucketStr = str(hostname)+":"+str(portNum)+"\t"
    # Check whether bucket exists
    try:
        curBucket = conn.get_bucket(bucketName)
    except boto.exception.S3ResponseError:
        return {"greeting": bucketStr}


    curObjKey = curBucket.get_key(keyName)
    
    bucketStr = str(bucketName)+"\t "+str(keyName)
    tempFilename = "temp.jpg"
    print ("\t bucketStr: %s "%(bucketStr))
    try:
        curImgObj = curObjKey.get_contents_to_filename(tempFilename)
    except AttributeError:
        return {"greeting": bucketStr}    

    filenames = []; keynames = []
    img = Image.open(tempFilename) 
    lx,ly = img.size

    scalingFactors = [1.5,0.75,0.5] 
    for idx,curScalingFactor in enumerate(scalingFactors):
        
        im1 = img.resize((int(lx*curScalingFactor), int(ly*curScalingFactor)), Image.NEAREST) 
        croppedImageFilename = str(keyName)+"_croppedRatio"+str(curScalingFactor)+".jpg"
        croppedKeyname = str(keyName)+"_croppedRatio"+str(curScalingFactor)
        filenames.append(croppedImageFilename)
        keynames.append(croppedKeyname)

        im1.save(croppedImageFilename,"jpeg")

    for idx in range(len(scalingFactors)):
        curCroppedKey = curBucket.new_key(keynames[idx])
        curCroppedKey.set_contents_from_filename(filenames[idx])

    bucketStr = str(bucketStr)+"\t modifiedKeyname --> "+str(curCroppedKey.name)+" "+str(curCroppedKey.size)+" "+str(curCroppedKey.last_modified)
    # In case we use the same container, there shouldn't be any issue for the next one.
    subprocess.check_output("rm "+str(tempFilename),shell=True)
    return {"greeting": bucketStr}

if __name__ == "__main__":
    main(sys.argv)
