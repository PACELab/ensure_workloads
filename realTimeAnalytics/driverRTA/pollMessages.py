import sys,time,requests,os,json,datetime
from kafka import KafkaConsumer
from kafka.structs import TopicPartition,OffsetAndMetadata
from requests.auth import HTTPBasicAuth

# To disable ssl warnings
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

authhandler = "TBD"
class pollQueue ():
    def __init__(self,actionName,messageQueue,keyValue,maxNumInvocations,batchSize,outputDir,parititonToMonitor,launchType,btwLaunchSleep,numReqsPerIter,verbose):
        self.daemon = True
        self.messageQueue = messageQueue
        self.keyValue = keyValue
        self.verbose = verbose
        self.actionName = actionName
        self.maxNumInvocations = maxNumInvocations
        self.parititonToMonitor = parititonToMonitor
        self.btwLaunchSleep = btwLaunchSleep
        self.numReqsPerIter = numReqsPerIter
        
        self.outputDir = outputDir
        self.activIDsFilename = str(self.outputDir)+"/activID_partition"+str(self.parititonToMonitor)+".log"
        launchTypeOptions = ["fixedInterval","realtime"]
        if(not launchType in launchTypeOptions):
            print ("\t launchType: %s is not one of the accepted launchTypeOptions: %s "%(launchType,launchTypeOptions))
        self.launchType = launchType
        self.batchSize = batchSize
        self.maxMessages = self.maxNumInvocations * self.batchSize

        self.issuesNumInvocations = 0 
        self.sendNumMessages = 0 
        self.pollingPeriod = 1 #in seconds
        self.printOffset = 2

        self.consumerTimeoutMS = 1000
        self.allActivationsInfo = [] # (activID, time)
        print ("\t <init> messageQueue: %s keyValue: %s maxNumInvocations: %d self.batchSize: %d self.parititonToMonitor: %d activIDsFilename: %s self.maxMessages: %d btwLaunchSleep: %s numReqsPerIter: %s "%(self.messageQueue,self.keyValue,self.maxNumInvocations,self.batchSize,self.parititonToMonitor,self.activIDsFilename,self.maxMessages,self.btwLaunchSleep,self.numReqsPerIter))
        self.getActionUrl()
        self.poll()

    def getActionUrl(self):
        # https://172.24.66.133/api/v1/namespaces/guest/actions/pathHello
        host = os.getenv('API_HOST')
        apiPrefix = "api/v1/namespaces/guest/actions"
        url = "https://"+str(host.strip())+"/"+str(apiPrefix)+"/"+str(self.actionName)
        self.actionUrl = url;
        self.authHandler = authhandler #HTTPBasicAuth("23bc46b1-71f6-4ed5-8c54-816aa4f8c502","123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")

    # return list of TopicPartition which represent the _next_ offset to consume
    def getOffsetList(self, messages):
        offsets = {}
        for message in messages:
            # Add one to the offset, otherwise we'll consume this message again.
            # That's just how Kafka works, you place the bookmark at the *next* message.

            print ("\t message.topic(): %s "%(message.topic))
            offsets[TopicPartition(message.topic,message.partition)] = OffsetAndMetadata(message.offset + 1,None)
            break;
        return offsets

    def publishActivationInfo(self):
        print ("\n\n Final activation info \n")
        activFile = open(self.activIDsFilename,"w")
        for curActivID,curActivLaunchTime in self.allActivationsInfo:
            #curActivLaunchTime = self.allActivationsInfo[curActivID]
            print ("\t activationID: %s \t time: %s "%(curActivID,curActivLaunchTime))
            activFile.write(str(curActivID)+"\n")
        print ("\t Closing activFile now..")
        activFile.close()

    def poll(self):
        #consumer = KafkaConsumer(self.messageQueue, auto_offset_reset='earliest',bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
        #consumer = KafkaConsumer(self.messageQueue,auto_offset_reset='earliest',bootstrap_servers=['bay15:9092'], consumer_timeout_ms=self.consumerTimeoutMS)
        consumer = KafkaConsumer(auto_offset_reset='earliest',bootstrap_servers=['server8:9092'], consumer_timeout_ms=self.consumerTimeoutMS)
        A = [TopicPartition(self.messageQueue, self.parititonToMonitor)]
        consumer.assign(A)
        partitions = consumer.assignment()
        if(self.verbose):
            idx = 0;
            for msg in consumer:
                idx+=1
                print ("\t idx: %d len(msg): %d msg: --%s-- "%(idx,len(msg.value),msg.value))
                break

        #consumer.config['group_id'] = self.actionName; partitions = consumer.assignment()

        
        print ("partitions: %s "%(str(partitions)))
        print (" Before while loop -- Offsets: begin: %s end: %s "%(consumer.beginning_offsets(partitions),consumer.end_offsets(partitions)))
        
        lastShippedOffset = list(consumer.beginning_offsets(partitions).values())[0]
        curEndOffset = list(consumer.end_offsets(partitions).values())[0]
        if(self.launchType == "realtime"):
            if(curEndOffset>3*self.batchSize):
                lastShippedOffset = curEndOffset-3*self.batchSize # Can get the cold start out of our way.
            else:
                lastShippedOffset = curEndOffset-1 # Assuming that wrapper is run before the producerRTA script
        elif(self.launchType == "fixedInterval"):
            lastShippedOffset = 0
            lastShippedOffset = list(consumer.beginning_offsets(partitions).values())[0]

        print ("\t Last shipped offset: %d "%(lastShippedOffset)); #sys.exit()
        consumer.seek(TopicPartition(self.messageQueue,self.parititonToMonitor),lastShippedOffset+1)
        #lastShippedOffset = 0
        
        idx = 0
        toSendJson = [] 
        while True:
            idx+=1;
            curEndOffset = list(consumer.end_offsets(partitions).values())[0]
            if((curEndOffset-lastShippedOffset) >= self.batchSize):
                if(self.issuesNumInvocations%5==0): print ("\t Yaay! found something to ship yo, cos curEndOffset: %d lastShippedOffset: %d "%(curEndOffset,lastShippedOffset))
                getRecords = consumer.poll(timeout_ms=self.consumerTimeoutMS,max_records=self.batchSize)
                #for key,value in getRecords.items(): # should change this to if condition.
                if(len(getRecords)>0):
                    #print ("\t getRecords: %s len(getRecords): %d "%(getRecords,len(getRecords)))
                    key = list(getRecords.keys())[0]
                    value = list(getRecords.values())[0]
                    #print ("\t key: --%s-- len(value): %d "%(key,len(value)))
                     
                    #consumer.commit(offsets=self.getOffsetList(value)) 
                    print ("\t len(toSendJson): %d value[0].offset: %d "%(len(toSendJson),value[0].offset))
                    for curMsg in value:
                        curDict = {}
                        curDict["topic"] = str(curMsg.topic)
                        curDict["partition"] = str(curMsg.partition)
                        curDict["offset"] = str(curMsg.offset)
                        curDict["key"] = str(curMsg.key)
                        curDict["value"] = str(curMsg.value)
                        toSendJson.append(curDict)
                    
                    print ("\t len(toSendJson): %d curMsg.offset: %d topic: %s partition: %d "%(len(toSendJson),curMsg.offset,curMsg.topic,curMsg.partition))

                    # time wsk action invoke --result pathHello --param name World -i
                    # response = requests.post(self.triggerURL, json=payload, auth=self.authHandler, timeout=10.0, verify=check_ssl)
                    startingOffset = lastShippedOffset
                    if(len(toSendJson)>=self.batchSize):
                        #print ("\t url --> %s "%(self.actionUrl))
                        
                        allResponses = []
                        curRespStart = 0; remainingLen = len(toSendJson)
                        while (remainingLen>0):
                            payload = {}
                            if(remainingLen<(1.5*self.batchSize)):
                                #if(remainingLen<self.batchSize): curDispatchSize = remainingLen
                                #else: curDispatchSize = self.batchSize
                                curDispatchSize = remainingLen
                            else:
                                curDispatchSize = self.batchSize 

                            curDispatchSize = remainingLen
                            payload["params"] = toSendJson[curRespStart:curDispatchSize-1]

                            for curReq in range(self.numReqsPerIter):
                                if(self.issuesNumInvocations%self.printOffset==0): print ("\t #reqs-issued: %d curReq: %s "%(self.issuesNumInvocations,curReq))
                                response = requests.post(self.actionUrl, auth=self.authHandler, json=payload, timeout=10.0, verify=False)
                                allResponses.append([response.status_code,curDispatchSize,response,datetime.datetime.now()])
                                time.sleep(self.btwLaunchSleep)

                            if(self.issuesNumInvocations<2):
                                time.sleep(7.5)
                            else:
                                time.sleep(self.btwLaunchSleep)

                            curRespStart+=curDispatchSize                    
                            remainingLen-=curDispatchSize
                            #print ("\t curDispatchSize: %d remainingLen: %d "%(curDispatchSize,remainingLen))

                        curRespStart = 0
                        for batchIdx,curRespSet in enumerate(allResponses):
                            #print ("\t curRespSet: %s "%(curRespSet))
                            curRespCode = curRespSet[0]
                            curDispatchSize = curRespSet[1]
                            curResp = curRespSet[2]
                            issuedTS = curRespSet[3]
                            #print ("\t response status_code: %s  curDispatchSize: %s "%(curRespCode,curDispatchSize))

                            if curRespCode in range(200, 300):
                                response_json = curResp.json()
                                if 'activationId' in response_json and response_json['activationId'] is not None:
                                    if(self.issuesNumInvocations%self.printOffset==0):
                                        print("[{}] Fired trigger with activationID {}".format(self.actionName, response_json['activationId']))
                                    self.allActivationsInfo.append([response_json['activationId'],issuedTS])
                                    """if(self.issuesNumInvocations<5):
                                        time.sleep(7.5)
                                    else:
                                        time.sleep(self.btwLaunchSleep)"""
                                else:
                                    print("[{}] Successfully fired trigger".format(self.actionName))                    
                                
                                #print ("\t Response json: --%s-- "%(str(response_json)))
                                if(batchIdx%self.numReqsPerIter==0):
                                    self.issuesNumInvocations+=1 
                                    idxOffset = curRespStart+curDispatchSize-1

                                #if(self.verbose): print ("\t idxOffset: %d toSendJson[idxOffset]\t[topic]: %s\t [offset]: %s "%(idxOffset,str(toSendJson[idxOffset]["topic"]),str(toSendJson[idxOffset]["offset"])))

                                # Assuming it's safe to seek until the point to which we have successfully processed. 
                                # This would mean, if not all actions are successful, we might end up rereading from the queue. This is fault tolerant, but not performant design.
                                    lastShippedOffset = int(toSendJson[idxOffset]["offset"])
                                    curRespStart+=curDispatchSize
                                    if(self.issuesNumInvocations%self.printOffset==0): print ("\t idxOffset: %d curRespStart: %d curDispatchSize: %d "%(idxOffset,curRespStart,curDispatchSize,))

                        # while seeking, I have to keep it +1, since I want the next record to the one I have already processed.
                        consumer.seek(TopicPartition(curMsg.topic,curMsg.partition),lastShippedOffset+1)
                        self.sendNumMessages+=(lastShippedOffset-startingOffset) # lastShippedOffset is not adjusted, so don't need +1 for counting.
                        #lastShippedOffset = curMsg.offset 

                        if(self.verbose): print ("\t Done with committing.. lastShippedOffset: %d self.issuesNumInvocations: %d self.sendNumMessages: %d "%(lastShippedOffset,self.issuesNumInvocations,self.sendNumMessages))
                        toSendJson = [] # Now that I have processed all the records that I have read, I will remove from the buffer.

                    else:
                        print ("\t idx: %d curEndOffset: %d lastShippedOffset: %d "%(idx,curEndOffset,lastShippedOffset))
                        time.sleep(self.pollingPeriod) 
                else:
                    print ("\t len(getRecords): %d "%(len(getRecords)))                       

            else:
                print ("\t idx: %d curEndOffset: %d lastShippedOffset: %d self.sendNumMessages: %d "%(idx,curEndOffset,lastShippedOffset,self.sendNumMessages))
                time.sleep(self.pollingPeriod)

            if( (self.issuesNumInvocations >= self.maxNumInvocations) or (self.sendNumMessages >= self.maxMessages)):
                break

        print ("\t End, self.issuesNumInvocations: %d idx: %d self.sendNumMessages: %d"%(self.issuesNumInvocations,idx,self.sendNumMessages))
        self.publishActivationInfo()
        if consumer is not None:
            consumer.close()        
