#! /usr/bin/python3
# some methods borrowed from Adnan Siddiqi's medium post: https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05
import time,datetime
import random, requests, sys
from kafka import KafkaProducer
import json

loremIpsum = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum."

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['server8:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, key, value,partitionToUse,verbose):
    publishRes = ""
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes,partition=partitionToUse)
        producer_instance.flush()
        #if(verbose): print()
        publishRes = 'Message published successfully.'
    except Exception as ex:
        #if(verbose):  print('Exception in publishing message')
        publishRes = 'Exception in publishing message'
        print(str(ex))
    return publishRes 

def generateRequest(allParams,Idx):
    # fields :  (time,lat, long, time_100m, num_vehicles_5m, temp_F, rain, snow, wind)
    curCityData = allParams[Idx]
    addOrSubtract = random.randint(0,1)
    curCityData['time'] = curCityData['prev']['time'] + random.random()*300
    timeDelta = random.randint(curCityData['time_min'],curCityData['time_max'])
    curCityData['time_100m'] = curCityData['prev']['time_100m']+timeDelta if addOrSubtract else curCityData['prev']['time_100m']-curCityData['time_min'] 

    vehDelta = random.randint(curCityData['numVehicles_min'],curCityData['numVehicles_max'])
    curCityData['numVehicles'] = curCityData['prev']['numVehicles']+vehDelta if addOrSubtract else curCityData['prev']['numVehicles']-curCityData['numVehicles_min'] 

    rainDelta = random.random()*curCityData['rain_rate']
    curCityData['rainInches'] = curCityData['prev']['rainInches']+rainDelta if addOrSubtract else curCityData['prev']['rainInches']-rainDelta

    snowDelta = random.random()*curCityData['snow_rate']
    curCityData['snowInches'] = curCityData['prev']['snowInches']+snowDelta if addOrSubtract else curCityData['prev']['snowInches']-snowDelta

    windRate = random.random()*curCityData['wind_rate']
    curCityData['wind'] = curCityData['prev']['wind']+windRate if addOrSubtract else curCityData['prev']['wind']-windRate

    curCityData['prev']['lat'] = curCityData['lat']
    curCityData['prev']['long'] = curCityData['long']
    curCityData['prev']['time'] =  curCityData['time'] 
    curCityData['prev']['time_100m'] =  curCityData['time_100m'] 
    curCityData['prev']['numVehicles'] =  curCityData['numVehicles_start'] 
    #curCityData['prev']['temp_F'] =  curCityData['temp_F'] 
    curCityData['prev']['rainInches'] =  curCityData['rain_start'] 
    curCityData['prev']['snowInches'] =  curCityData['snow_start'] 
    curCityData['prev']['wind'] =  curCityData['wind_start'] 

    retJsonData = json.dumps(curCityData['prev'])
    return retJsonData

def generateRanges(numCities):
    allParams = []

    for curCityIdx in range(numCities):
        curCityData = {}
        curCityData['lat'] = random.random()*90
        curCityData['long'] = random.random()*180
        curCityData['time'] = ( datetime.datetime.now() - datetime.datetime(1970,1,1)).total_seconds() # time since epoch.
        startPoint = int(random.randint(1,30))
        curCityData['time_100m'] = startPoint
        curCityData['time_max'] = random.randint(int(startPoint*0.2),int(startPoint*5.0))
        curCityData['time_min'] = int(0.2*curCityData['time_max'])

        startPoint = int(random.randint(1,10000))
        curCityData['numVehicles_start'] = startPoint;
        curCityData['numVehicles_max'] = (random.randint(int(startPoint*0.2),int(startPoint*2.0))) # min is 20% of max.
        curCityData['numVehicles_min'] = int(0.2*curCityData['numVehicles_max']) # min is 20% of max.
        curCityData['rain_start'] = int(random.randint(1,20))
        curCityData['rain_rate'] = random.random()*0.5 # min is 20% of max.
        curCityData['snow_start'] = int(random.randint(1,20))
        curCityData['snow_rate'] = random.random()*0.5 # min is 20% of max.
        curCityData['wind_start'] = int(random.randint(1,20))
        curCityData['wind_rate'] = random.random()*0.25 # min is 20% of max.

        curCityData['prev'] = {}
        curCityData['prev']['time'] =  curCityData['time'] 
        curCityData['prev']['time_100m'] =  curCityData['time_100m'] 
        curCityData['prev']['numVehicles'] =  curCityData['numVehicles_start'] 
        #curCityData['prev']['temp_F'] =  curCityData['temp_F'] 
        curCityData['prev']['rainInches'] =  curCityData['rain_start'] 
        curCityData['prev']['snowInches'] =  curCityData['snow_start'] 
        curCityData['prev']['wind'] =  curCityData['wind_start'] 
        allParams.append(curCityData)

        if(curCityIdx==0):
            print ("curCityData: %s "%(curCityData))

    return allParams

def main(argv):
    numArgs = 6
    if(len(argv)<numArgs):
        print ("\t Usage:  <message-queue-name> <key-string> <numRequestsPerSec> <numCities> <partitionToUse> <verbose>\n")
        sys.exit()

    messageQueue = argv[1]
    keyValue = argv[2]
    numRequestsPerSec = int(argv[3])
    numCities = int(argv[4])
    maxNumMessages = int(argv[5])
    partitionToUse = int(argv[6])
    
    verbose = False
    if(int(argv[7])>0):
        verbose = True
    lenLoremIpsum = len(loremIpsum)

    kafka_producer = connect_kafka_producer()
    numMessages = 0

    allParams = generateRanges(numCities)

    print ("\t numRequestsPerSec: %d numCities: %d partitionToUse: %d "%(numRequestsPerSec,numCities,partitionToUse))
    begin = time.time();
    while(True):
        # we want on average, numRequestsSec or in time 1/numReqsSec; avg = (a+b)/2 ; a = 0; avg = b/2; b = 2*avg ==> b = 2/numReqsSec
        sleepTime = random.uniform(0,2/numRequestsPerSec); #sleep time in uSeconds. between 0 to 1 # 0 to 1 is scaled to 0 to 0.1, i.e. ensuring atleast a message is added every numRequestsPerSec ms.
        time.sleep(sleepTime) 

        #curMessageLen = int(random.random()*lenLoremIpsum); curMessageLen = curMessageLen-1 if (curMessageLen>0) else 10
        #print ("\t Slept for %.6f s and the message would be: %s "%(sleepTime,loremIpsum[0:curMessageLen]))
        #publish_message(kafka_producer, messageQueue, keyValue, loremIpsum[0:curMessageLen],verbose)        

        toPublishReq = generateRequest(allParams,random.randint(0,numCities-1))
        curMessageLen = len(toPublishReq)
        
        publishRes = publish_message(kafka_producer, messageQueue, keyValue, toPublishReq,partitionToUse,verbose)        
        if(numMessages%200==0): 
            if(verbose): print ("\t Slept for %.6f s numMessages: %d and the messageLen would be: %d and publishRes: %s "%(sleepTime,numMessages,curMessageLen,publishRes)) # toPublishReq,

        numMessages+=1
        if(numMessages>maxNumMessages):
            break;
    end = time.time();
    totalTime = end-begin
    if kafka_producer is not None:
        kafka_producer.close()
    print ("\t Sent: %d messages end: %s begin: %s totalTime: %s"%(numMessages,end,begin,totalTime))

if __name__ == "__main__":
    main(sys.argv)
