#This code gets the input data segment and sorts them
import json,sys
import time,random,subprocess,copy,gc

import boto3
from botocore.exceptions import ClientError

aws_access_id = "TBD"
aws_secret_id = "TBD" #os.getenv('AWS_SECRET_ID',"")

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

    # for i in range(retries):
    #     try:
    #         s3 = boto3.client('s3')
    #         with open("FILE_NAME", "rb") as f:
    #             s3.upload_fileobj(f, "BUCKET_NAME", "OBJECT_NAME")
    #     except Exception as exception:
    #         print ("\t Couldn't upload the key: %s to bucket: %s iter: %d, error: %s "%(keyName,bucketName,i,exception))

def readObject(params,args):
    name = args.get("params","invalid")
    if(name!="invalid"):
        paramsArray = json.dumps(name)
        msg = json.loads(paramsArray)
        params["filePrefix"] = msg["filePrefix"]
        params["numParts"] = msg["numParts"]
        params["numFiles"] = msg["numFiles"]
        params["bucketName"] = msg["bucketName"]
    else:
        #return "Error: params not found!! name -->"+str(name)
        params["filePrefix"] = "medIp" 
        params["numParts"] = 6
        params["numFiles"] = 32
        params["bucketName"] = "sunbucket"


        tempNum = random.randint(1,8)
        params["filePrefix"] = str(tempNum)+"sortinput" 
        params["numParts"] = 8
        params["numFiles"] = 8
        params["bucketName"] = "ow-aws-sorting"

    return "Alright"


def customSortKeyFunc(record):
    return (record[:10])

def sortInput(filePrefix,numParts,numFiles,bucketName):
    print(f'File Name Received at sortInput - {filePrefix}')
    try:
        #startPart = random.randrange(0,numFiles-numParts-1); endPart = startPart+numParts
        startPart = 1; endPart = startPart+numParts

        start = time.time()
        gcThd = gc.get_threshold()
        print ("\t gcThd: %s "%(str(gcThd)))
        numSplits = 2
        numRepeatIters = 3
        perSplitParts = numParts//numSplits
        fileContentLen = 0
        for curSplit in range(numSplits):
            allLines = []
            startPart = 1+(curSplit*perSplitParts); 
            endPart = startPart+perSplitParts

            print ("\t curSplit: %d "%(curSplit))
            fileReadAccum = 0.0; jumblingAccum = 0.0
            for curPartNum in range(startPart,endPart):  
                #fileName = filePrefix+'.p'+str(curPartNum)
                start = time.time()
                fileName = filePrefix+str(curPartNum)+'.txt'
                destFileName = 'Final' + str(fileName)
                pullFileFromStorage(bucketName, fileName, destFileName) # storagemodule.pullFileFromStorage(conn, bucketName, fileName, destFileName)
                fp = open(destFileName)
                fileContent = fp.readlines()

                # aLen = len(fileContent)
                # temp = copy.copy(fileContent)
                # random.shuffle(temp)

                # for curIdx in range(aLen):
                #     temp[curIdx] = int(temp[curIdx].strip())
                #     fileContent[curIdx] = int(fileContent[curIdx].strip())
                #     if(random.random()>0.5):
                #         temp[curIdx] = temp[curIdx] + (temp[curIdx]//2)
                #     else:
                #         temp[curIdx] = temp[curIdx] - (temp[curIdx]//2)

                # allLines = allLines + fileContent + temp

                fileReadEnd = time.time()
                fileReadAccum+= (fileReadEnd-start)

                start = time.time()
                aLen = len(fileContent)
                fileContentLen = aLen
                for curIdx in range(aLen):
                    try:
                        fileContent[curIdx] = int(fileContent[curIdx].strip())
                    except AttributeError as e:
                        continue;

                jumbled = copy.copy(fileContent)
                random.shuffle(jumbled)
                temp = []

                for curRepIter in range(numRepeatIters):
                    temp = temp + jumbled
                    random.shuffle(jumbled)

                for curIdx in range(numRepeatIters * aLen):
                        coinFlip = random.random()
                        if(coinFlip):
                            temp[curIdx] = temp[curIdx] + (temp[curIdx]//coinFlip)
                        else:
                            temp[curIdx] = temp[curIdx] - (temp[curIdx]*coinFlip)
                        temp[curIdx] = int(temp[curIdx])

                allLines = allLines + fileContent + temp

                jumbleEnd = time.time()
                jumblingAccum+= (jumbleEnd-fileReadEnd)

            print("Getting entire file takes {%.3f}s jumblingAccum: {%.3f}s "%(fileReadAccum,jumblingAccum))
            gc.collect()

            start = time.time()
            allLines.sort() 
            end = time.time()
            print("Sorting entire file takes {%.3f}"%(end - start))

            totalLen = len(allLines); 
            sliceLen = len(allLines)//(perSplitParts) #*2)
            idx = 0
            for curPartNum in range(startPart,endPart):  
                start = time.time()
                #fileName = filePrefix+'.p'+str(curPartNum)
                fileName = filePrefix+str(curPartNum)+'.txt'
                randID = random.randint(1,9999)
                destFileName = 'sorted_'+str(filePrefix)+str(curPartNum)+"_"+str(randID)+'.txt'

                startIdx = (idx)*sliceLen
                endIdx = startIdx+sliceLen
                idx+=1

                fp = open(destFileName,'w')
                numElements = 0
                for curEle in allLines[startIdx:endIdx]:
                    if(numElements>fileContentLen):
                        break;
                    fp.write(str(curEle)+"\n")
                    numElements+=1
                
                fp.close()
                pushFileToStorage(bucketName,destFileName,destFileName)

                try:
                    subprocess.check_output("rm "+str(destFileName),shell=True,universal_newlines=True)
                    print ("\t Deleted file: %s "%(destFileName))
                except Exception as e:
                    print ("\t Error: %s  while deleting file: %s "%(e,destFileName))            

                end = time.time()
                print("curPartNum: %d Writing+Pushing to ceph takes {%.3f}"%(curPartNum,end - start))
                gc.collect()

    except IOError as error:
        print(f'There is no such file or directory. It is possible that there were no numbers in that bucket.Error message - {error}')

def main(args):

    params = {}
    res = readObject(params, args)

    if (res != "Alright"):
        return {"greetings": "some issue with parsing the input parameters. ErrorMsg --> " + str(res)}

    # Number of random numbers to be generated for each sample
    filePrefix = (params["filePrefix"])
    print('Source File Prefix - {}'.format(filePrefix))

    numParts = (params["numParts"])
    try:
        numParts = int(numParts.strip())
    except Exception as exception:
        numParts = int(numParts)    
    print('numParts - {}'.format(numParts))

    numFiles = (params["numFiles"])
    try:
        numFiles = int(numParts.strip())
    except Exception as exception:
        numFiles = int(numFiles)
    print('numFiles - {}'.format(numFiles))
    

    bucketName = (params["bucketName"])
    print('Bucket Name - {}'.format(bucketName))

    sortInput(filePrefix,numParts,numFiles,bucketName)
    return {"greetings": "Successful"}
