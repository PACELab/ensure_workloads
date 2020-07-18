#! /usr/bin/python

import sys,json,time,random
import objcrypt
from rediscluster import RedisCluster
import boto3

def pullFileFromStorage(bucketName,keyName,destFileName):
    print('Pulling File {} from local to bucket {} with key name {}'.format(keyName,bucketName,keyName))
    retries = 3

    s3rsc = boto3.resource('s3',aws_access_key_id=aws_access_id,aws_secret_access_key=aws_secret_id)
    for i in range(retries):
        try:
            obj = s3rsc.Object(bucketName,keyName) 
            obj.download_file(destFileName)
            break
        except Exception as exception:
           print ("\t Couldn't download the key: %s to bucket: %s iter: %d "%(keyName,bucketName,i))

def pushFileToStorage(bucketName,keyName,filename):
    print('pushFileToStorage File {} from local to bucket {} with key name {}'.format(keyName,bucketName,keyName))
    retries = 3
    s3clt = boto3.client('s3',aws_access_key_id=aws_access_id,aws_secret_access_key=aws_secret_id)
    for i in range(retries):
        try:
            response = s3clt.upload_file(filename,bucketName,keyName)
            break
        #except ClientError as e:
        except Exception as exception:
            print ("\t Couldn't upload the key: %s to bucket: %s iter: %d, error: %s "%(keyName,bucketName,i,exception))

def main(args):

    start = time.time()
    #name = args.get("params", "stranger")
    name = "stranger"

    prefixNum = 0
    numIpFiles = 1000
    numKeys = 1
    numChunks = 1

    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)

        print ("\t msg: %s "%(str(msg)))
        bucketName = msg["bucketName"] 
        numKeys = msg["numKeys"]
        numChunks = msg["numChunks"]
    else:
        bucketName = "ow-aws-fe"
        numKeys = 5
        numChunks = 5

    prefixNum = random.randrange(1,numIpFiles)
    if(numKeys<0):
        numKeys = 1

    if(numChunks<0):
        numChunks = 1

    keyName = "data"+str(prefixNum)+".p0"

        #return "Error: params not found!! name -->"+str(name)

    userDetails = {
        "user": "emailGen",
        "access_key": access_key,
        "secret_key": secret_key,
        "host" : host-ip, #bay16
        "port" : port-num,    
    }

    access_key = userDetails["access_key"]
    secret_key = userDetails["secret_key"]
    hostname = userDetails["host"]
    portNum = userDetails["port"]

    bucketStr = hostname
    bucketStr = str(hostname)+":"+str(portNum)+"\t"
    
    hosts = [{"host": redis-host-1, "port": "7000"}, {"host": redis-host-2, "port": "7001"}, {"host": redis-host-3, "port": "7002"}]

    startup_nodes = [hosts[random.randint(0,2)]]
    #rc = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

    tempFilename = "temp.log"
    encryptAccum = 0; dictionary = {}; 

    for idx in range(numKeys):
        prefixNum = random.randrange(0,numIpFiles-1)
        keyName = "kv_"+str(prefixNum)+".log"

        bucketStr = str(bucketName)+"\t "+str(keyName)
        # curObjKey = curBucket.get_key(keyName)
        # print ("\t idx: %d keyName: %s "%(idx,bucketStr))

        # try:
        #     curJsonObj = curObjKey.get_contents_to_filename(tempFilename)
        # except AttributeError:
        #     return {"greeting": bucketStr}   

        pullFileFromStorage(bucketName,keyName,tempFilename)

        tempDict = json.load(open(tempFilename)) 
        dictLen = len(tempDict)
        sliceLen = dictLen//(numKeys) # should check numKeys?

        if(sliceLen<1):
            sliceLen = 2
        startIdx = idx*sliceLen 
        endIdx = startIdx+(sliceLen//2)
        tempArr = list(tempDict.keys())
        print ("\t idx: %d sliceLen: %d len(tempArr): %d startIdx: %d endIdx: %d "%(idx,sliceLen,len(tempArr),startIdx,endIdx))
        
        reqdItems = tempArr[startIdx:endIdx]
        for keyIdx,curKey in enumerate(reqdItems): 
            newKey = str(curKey)+"_"+str(idx)
            #if(keyIdx%50==0): print ("\t keyIdx: %d curKey: %s newKey: %s "%(keyIdx,curKey,newKey))
            dictionary[newKey] = tempDict[curKey]

    encryptBegin = time.time()
    crypter = objcrypt.Crypter('key', 'cbc')
    #dictionary = json.load(open(tempFilename))

    encrypted_dict = crypter.encrypt_object(dictionary)
    dictStr = json.dumps(dictionary)
    enc_json = crypter.encrypt_json(dictStr) #json_dict)

    encryptEnd = time.time()
    encryptAccum = encryptAccum + (encryptEnd-encryptBegin)

    encLen = len(enc_json)
    print ("\n\t len(dictionary): %d len(enc_json): %d "%(len(dictionary),len(enc_json)))
    
    for idx in range(numChunks):
        sliceLen = encLen//(numChunks) # should check numKeys?

        if(sliceLen<1):
            sliceLen = 2
        startIdx = idx*sliceLen 
        endIdx = (idx+1)*sliceLen-1

        slicedEncJson = enc_json[startIdx:endIdx]
        opFilenames = ["op.log"]

        opKeyName = "key"+str(random.randrange(0,10000))
        opKey = ["op_"+str(opKeyName)+".log"]

        opFile = open(opFilenames[0],'w')

        opFile.write(str(enc_json)+"\n")
        opFile.close()
        #writeEncryptedFile(curBucket,opKey,opFilenames)
        print ("\t len(dictionary): %d pushing opKey: %s len(slicedEncJson): %s "%(len(dictionary),opKeyName,len(slicedEncJson)))
        rc.set(opKeyName,enc_json)
        
    end = time.time()
    timeTaken = end-start
    encryptTime = encryptAccum #dbPush-encryptBegin

    bucketStr = str(bucketStr)+"\t time: "+str('%.3f'%(timeTaken))+"\t encryptTime: "+str('%.3f'%(encryptTime))
    print ("\t bucketStr: %s "%(bucketStr))
    return {"greeting": bucketStr}

if __name__ == "__main__":
    main(sys.argv)
