#import json,socket
import rediscluster,json,random

congestionLimits = {
    'snowInches' : 2,
    'rainInches' : 2,
    'wind' : 2,
    'numVehicles' : 1000, # divide it by this value.
    'time_100m' : 10,
    'overall_limit' : 150

}
def checkCongestion(value):
    congestionScore = 0.0

    if(value["time_100m"] > congestionLimits['time_100m']):
        congestionScore+=100+(value["time_100m"]/congestionLimits['time_100m'])
        #print ("\t 0. congestionScore: %.3f "%(congestionScore))
    if(value["numVehicles"] > congestionLimits['numVehicles']):
        congestionScore+=(10* value["numVehicles"]/congestionLimits['numVehicles'])
        #print ("\t 1. congestionScore: %.3f "%(congestionScore))
    if(value["snowInches"] > congestionLimits['snowInches']):
        congestionScore+=15+(value["snowInches"]/congestionLimits['snowInches'])
        #print ("\t 2. congestionScore: %.3f "%(congestionScore))
    if(value["rainInches"] > congestionLimits['rainInches']):
        congestionScore+=15+(value["rainInches"]/congestionLimits['rainInches'])
        #print ("\t 3. congestionScore: %.3f "%(congestionScore))
    if(value["wind"] > congestionLimits['wind']):
        congestionScore+=15+(value["wind"]/congestionLimits['wind'])
        #print ("\t 4. congestionScore: %.3f "%(congestionScore))

    if(congestionScore>congestionLimits['overall_limit']):
        key = str(value["lat"])+"_"+str(value["long"])+"_"+str(value["time"])
        key = round(value["lat"]+value["long"],3)
        print ("\t <END>. key: %.3f congestionScore: %.3f "%(key,congestionScore))
        value['congestionScore'] = congestionScore
        return (1,[key,str(value)])
    else:
        return (0,congestionScore)

def strToDict(value):
    value = value.replace("{", ""); 
    value = value.replace("}", "") 
    value = value.replace("'","")
    value = value.replace("b","")
    value = value.split(",")
    temp = {}
    for curEle in value:
        b = curEle.split(":")
        b[0] = b[0].replace('"',"").strip()
        #print ("\t b[0]: --%s-- b[1]: %s "%(b[0],b[1]))
        temp[b[0]] = float(b[1].strip())
    value = temp
    return value

def main(args):
    name = args.get("params", "stranger")
    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)
        len_msg = len(messagesArr)
        value = " This is the default value msg"

        numMessages = 0
        try:
            len2_msg = len(msg)
            len3_msg = len(msg[0])
            numMessages = len(msg)
            len4_msg = len(msg[0]["topic"])
            offset = msg[0]["offset"]
            needDefVals = 0                
        except (KeyError,IndexError):
            needDefVals = 1

        if(needDefVals):
            offset = 123
            len2_msg = 1
            len3_msg = 2
            len4_msg = 345
        toSetKey = offset
        toSetVal = str(len2_msg)+" "+str(len3_msg)+" "+str(len4_msg)
    else:
        messagesArr = "stranger"
        len_msg = len(name)
        toSetKey = "default123"
        toSetVal = "no-message-received!"
    
    if(needDefVals==0):
        toUpdateMsgs = []
        for curMsg in msg:
            value = strToDict(curMsg["value"])
            (shouldUpdate,toUpdateMsg) = checkCongestion(value)
            if(shouldUpdate):
                toUpdateMsgs.append(toUpdateMsg)

    hosts = [{"host": redis-host-1, "port": "7000"}, {"host": redis-host-2, "port": "7001"}, {"host": redis-host-3, "port": "7002"}]
    #hostname = "172.24.202.10";portnum = "7000"

    startup_nodes = [hosts[random.randint(0,2)]]
    rc = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    fooVal = rc.get("foo")
    print(rc.set(toSetKey,toSetVal))

    usedKey = "None"
    for curMsgSet in toUpdateMsgs:
        usedKey = curMsgSet[0]
        rc.set(curMsgSet[0],curMsgSet[1])
    
    greeting = "Hello len(name): --> " + str(len_msg) +"\n toSetKey --> "+str(toSetKey)+"\t toSetVal --> "+str(toSetVal)
    try:
        snowInches = value["snowInches"]
    except KeyError:
        snowInches = "NoNoNo"

    greeting = str(greeting)+" startup_nodes['host'] --> "+str(startup_nodes[0]['host'])+" btw foo val is "+str(fooVal)+" usedKey --> "+str(usedKey)
    return {"greeting": greeting}
