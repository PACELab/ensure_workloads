#! /usr/bin/python3

import sys,math,json
import time,subprocess
import boto
import boto.s3.connection


access_key = "TBD"
secret_key = "TBD" #os.getenv('AWS_SECRET_ID',"")
hostname = "TBD" 
portNum = "TBD"

constVars = {
    "REC_LENGTH" : 49,
    "dbName_length" : 64,
}

dbFields = {
    'year'      : 0,
    'm0'        : 1,
    'm1'        : 2,
    'm2'        : 3,
    'm3'        : 4,
    'name'      : 5, 
    'lat'       : 6,
    'long'      : 7,
    'm5'        : 8,
    'fields'    : 9
}

def readDistFilename(allParams,opFilename):
    readFile = open(opFilename,'r').readlines()
    minFields = 3 # lat, long, dist
    for curLine in readFile:
        curLine = curLine.strip().split(",")
        if(len(curLine)!=minFields): 
            return "Error: curLine: %s doesn't have %d fields"%(curLine,minFields)
        tempDict = {}
        tempDict['lat'] = float(curLine[0])
        tempDict['long'] = float(curLine[1])
        tempDict['dist'] = float(curLine[2])
        allParams['collected_topN'].append(tempDict)

def findMergedTopN(allParams):
    mergedTopN = []
    collectedDataLen = len(allParams['collected_topN'])

    for curIdx in range(allParams["ip_N"]):
        curMinIdx = curIdx; minDist = 100*1000*1000
        # Have found 'curIdx-1' min distances, now searching for 'curIdx'th min distance.
        for curLocIdx in range(curIdx,collectedDataLen):
            # Find the index at which we have 'curIdx' th min distance, then swap it to the curIdx location.
            if(allParams['collected_topN'][curLocIdx]['dist'] < minDist):
                minDist = allParams['collected_topN'][curLocIdx]['dist']
                curMinIdx = curLocIdx
        #Swap
        if(curMinIdx != curIdx): # if they are the same, nothing to swap!
            temp = {}
            temp ['lat'] = allParams['collected_topN'][curMinIdx]['lat']
            temp ['long'] = allParams['collected_topN'][curMinIdx]['long']
            temp ['dist'] = allParams['collected_topN'][curMinIdx]['dist']

            allParams['collected_topN'][curMinIdx]['lat'] = allParams['collected_topN'][curIdx]['lat']
            allParams['collected_topN'][curMinIdx]['long'] = allParams['collected_topN'][curIdx]['long']
            allParams['collected_topN'][curMinIdx]['dist'] = allParams['collected_topN'][curIdx]['dist']

            allParams['collected_topN'][curIdx]['lat'] = temp['lat']
            allParams['collected_topN'][curIdx]['long'] = temp['long']
            allParams['collected_topN'][curIdx]['dist'] = temp['dist']

    merged_topDists = []
    for curIdx in range(allParams["ip_N"]):
        mergedTopN.append(allParams['collected_topN'][curIdx])
        merged_topDists.append(allParams['collected_topN'][curIdx]['dist'])
    allParams['mergedTopN'] = mergedTopN
    allParams['merged_topDists'] = merged_topDists

    
def readFile(path,allParams):
    readFile = open(path,"r").readlines()
    curPartData = []
    for lineNum,curLine in enumerate(readFile):
        tempDict = {}
        #tempDict['lat'] = 
        curLine = curLine.strip().split(" ")
        temp = []
        for curTerm in curLine:
            if(curTerm!=""):
                temp.append(curTerm)
        curLine = temp
        if(len(curLine)<dbFields['fields']):
            print ("\t Error: curLine: %s has fewer than %d fields "%(curLine,dbFields['fields']))
            continue;
        tempDict['lat'] = float(curLine[dbFields['lat']])
        tempDict['long'] = float(curLine[dbFields['long']])
        curPartData.append(tempDict)
        #print ("\t lineNum: %d curLine: --%s-- len(curLine): %d lat: %s long: %s "%(lineNum,curLine,len(curLine),curLine[dbFields['lat']],curLine[dbFields['long']]))
    allParams['data'] = curPartData
    return

def calcDistance(allParams):
    for curLoc in allParams['data']:
        dist = ((curLoc['lat']-allParams['ip_lat']) *(curLoc['lat']-allParams['ip_lat']))
        dist+= ((curLoc['long']-allParams['ip_long']) * (curLoc['long']-allParams['ip_long']))
        dist = round(math.sqrt(dist),4)
        curLoc['dist'] = dist

def findTopN(allParams):
    topN = []
    dataLen = len(allParams['data'])
    for curIdx in range(allParams["ip_N"]):
        curMinIdx = curIdx; minDist = 100*1000*1000
        # Have found 'curIdx-1' min distances, now searching for 'curIdx'th min distance.
        for curLocIdx in range(curIdx,dataLen):
            # Find the index at which we have 'curIdx' th min distance, then swap it to the curIdx location.
            if(allParams['data'][curLocIdx]['dist'] < minDist):
                minDist = allParams['data'][curLocIdx]['dist']
                curMinIdx = curLocIdx
        #Swap
        if(curMinIdx != curIdx): # if they are the same, nothing to swap!
            temp = {}
            temp ['lat'] = allParams['data'][curMinIdx]['lat']
            temp ['long'] = allParams['data'][curMinIdx]['long']
            temp ['dist'] = allParams['data'][curMinIdx]['dist']

            allParams['data'][curMinIdx]['lat'] = allParams['data'][curIdx]['lat']
            allParams['data'][curMinIdx]['long'] = allParams['data'][curIdx]['long']
            allParams['data'][curMinIdx]['dist'] = allParams['data'][curIdx]['dist']

            allParams['data'][curIdx]['lat'] = temp['lat']
            allParams['data'][curIdx]['long'] = temp['long']
            allParams['data'][curIdx]['dist'] = temp['dist']

    topDists = []
    for curIdx in range(allParams["ip_N"]):
        topN.append(allParams['data'][curIdx])
        topDists.append(allParams['data'][curIdx]['dist'])
    allParams['topN'] = topN
    allParams['topDists'] = topDists

def getConn(allParams):
    conn = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = hostname,
            port = portNum,
            is_secure=False,               # uncomment if you are not using ssl
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )
    allParams['conn'] = conn
    return conn

def readObject(allParams,args):

    name = args.get("params", "stranger")
    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)

        bucketName = msg["bucketName"] #args.get("bucketName","cfd_data") 
        keyName = msg["keyName"] #args.get("keyName","fvcorr.domn.193K") 
        numParts = msg["numParts"] #args.get("keyName","fvcorr.domn.193K")  
        keySuffix = msg["keySuffix"] #args.get("keyName","fvcorr.domn.193K")  
        curPartNum = msg["curPartNum"]
        ip_lat = msg["ip_lat"]
        ip_long = msg["ip_long"]
        ip_N = msg["ip_N"]
    else:
        return "Error: params not found!! name -->"+str(name)

    #constVars['numParts'] = 2 # hack, should be passed during invocation.

    conn = getConn(allParams)
    # Create bucket and an object
    try: 
        bucket = conn.get_bucket(bucketName)
    except boto.exception.S3ResponseError:
        return "Error: bucket missing "

    key = bucket.get_key(keyName)
    localPath = 'temp_'+str(keyName)+".log"
    try:
        key.get_contents_to_filename(localPath)    
    except AttributeError:
        return "Error: file/key missing "        

    allParams['path'] = localPath
    allParams['conn'] = conn
    allParams["ip_lat"] = ip_lat
    allParams["ip_long"] = ip_long
    allParams["ip_N"] = ip_N
    allParams['bucketName'] = bucketName
    allParams['curPartNum'] = curPartNum
    allParams['keySuffix'] = keySuffix
    allParams['numParts'] = numParts
    allParams["fluxKey"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])
    allParams["distFilename"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])+".log"
    return "Alright!"

def synchronizeDistances(allParams):
    #push fluxes. 
    #conn = allParams['conn'] #
    conn = getConn(allParams)
    try: 
        bucket = conn.get_bucket(allParams['bucketName'])
    except boto.exception.S3ResponseError:
        return "Error: bucket missing "

    #tempFilename = str(allParams['keySuffix'])+"_"str(allParams["curPartNum"])+".log"
    with open(allParams["distFilename"], 'w') as f:
        for item in allParams["topN"]:
            f.write("%s,%s,%s\n" % (item['lat'],item['long'],item['dist']))

    myFluxKey = bucket.new_key(allParams["fluxKey"])
    myFluxKey.set_contents_from_filename(allParams["distFilename"])

    print ("\t In sync str(allParams[keySuffix]): %s "%(str(allParams["keySuffix"])))
    # wait     
    if(allParams['curPartNum'] == 0): # only one thread will merge everything.
        startPartNum = 0; allFilesNotAccessed=True
        numFilesAccessed = 1; # I already have my file.
        listOfPartsFound = [allParams['curPartNum']] # commented as hack.
        while allFilesNotAccessed:
            if(numFilesAccessed==allParams['numParts']):
                allFilesNotAccessed = False
            for curPartNum in range(startPartNum,constVars['numParts']):
                if(curPartNum in listOfPartsFound): continue; # commented as hack, since I have only one part.

                # doing this as a hack to just check the logic. Should test it on serverless-cluster.
                curDistKeyname = str(allParams["keySuffix"])+"_"+str(curPartNum)
                opFilename = str(allParams["keySuffix"])+"_"+str(curPartNum)+".log" #(doing "+1" as a hack for testing.)
                print ("\t curPartNum: %d allParams[fluxKey]: %s curDistKeyname: %s "%(curPartNum,allParams["fluxKey"],curDistKeyname))
                curFluxKey = bucket.get_key(curDistKeyname)
                try:
                    curFluxKey.get_contents_to_filename(opFilename)
                    listOfPartsFound.append(curPartNum)
                    #startPartNum+=1 # will only check for other cases, which haven't been accessed.
                    numFilesAccessed+=1
                    print ("\t Found dist file %s for key--> %s "%(opFilename,curDistKeyname))
                    if(numFilesAccessed==allParams['numParts']):
                        allFilesNotAccessed = False
                    break ;
                except AttributeError:
                    time.sleep(0.1) #  
                    #break; # will poll on only one file.
                    continue # can use this to ensure all the keys are accessed instead of polling on one of them.
            
        # if part0, remove all the flux from this iteration.
        print ("\t now I have finished receiving files from all of them! ")
    
        allParams['collected_topN'] = []
        for curPartNum in range(constVars['numParts']):
            if(curPartNum!=allParams['curPartNum']):
                opFilename = str(allParams["keySuffix"])+"_"+str(curPartNum)+".log"
                readDistFilename(allParams,opFilename)
            else: #this is my part, so copy inmemory ds.
                for curLoc in allParams['topN']:
                    allParams['collected_topN'].append(curLoc)
            print ("\t curPartNum: %d len(allParams['collected_topN']): %d "%(curPartNum,len(allParams['collected_topN'])))

        for curPartNum in range(constVars['numParts']):
            curDistKeyname = str(allParams["keySuffix"])+"_"+str(curPartNum)
            bucket.delete_key(curDistKeyname)
            opFilename = str(allParams["keySuffix"])+"_"+str(curPartNum)+".log" 
            subprocess.check_output("rm "+str(opFilename),shell=True)
    return 

def main(args):
    allParams = {}
    res = readObject(allParams,args)
    if(res!="Alright!"):
        return {"greetings":"some issue with reading the file. ErrorMsg --> "+str(res)}
    constVars['numParts'] = allParams['numParts'] 
    
    # should remove when we uncomment the above part
    
    """allParams['path'] = "part_1.db"
    allParams['numParts'] = 2 # should come in as a parameter 
    constVars['numParts'] = allParams['numParts']
    allParams['ip_lat'] = 30
    allParams['ip_long'] = 90
    allParams['ip_N'] = 15
    allParams['bucketName'] = "nn_data"
    allParams['keySuffix'] = "nn_np2_part"
    allParams['curPartNum'] = 0
    constVars['numParts'] = allParams['numParts']
    allParams["fluxKey"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])
    allParams["distFilename"] = str(allParams['keySuffix'])+"_"+str(allParams["curPartNum"])+".log" """

    print ("\t path: %s "%(allParams['path']))
    begin = time.clock()
    #print ("\t allParams['ff_flux_contribution_momentum_x']: %s "%(allParams['ff_flux_contribution_momentum_x']))
    readFile(allParams['path'],allParams)
    print ("\t len(allParams['data']: %d "%(len(allParams['data'])))
    calcDistance(allParams)
    print ("\t len(allParams['data']: %d len(allParams['data'][41]): %d"%(len(allParams['data']),len(allParams['data'][41])))    
    findTopN(allParams)
    print ("\t len(allParams['topN']: %d "%(len(allParams['topN'])))    
    synchronizeDistances(allParams)

    if(allParams['curPartNum'] ==0):
        findMergedTopN(allParams)
        end = time.clock();
        diff = end-begin        
        return {"greetings": "Time taken: %.3lf \n My topDists--> %s\n Merged topDists--> %s "%(diff,allParams['topDists'],allParams['merged_topDists'])}
    else:
        end = time.clock();
        diff = end-begin        
        return {"greetings": "Time taken: %.3lf \n My topDists--> %s "%(diff,allParams['topDists'])}
    
if __name__ == "__main__":
    retVal = main(sys.argv)
    print ('\t retVal: %s '%(retVal))
