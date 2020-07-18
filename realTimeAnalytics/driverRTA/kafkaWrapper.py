#! /usr/bin/python3

import sys
from pollMessages import pollQueue 

# Init plan: 
# Create a main object. It will have a function, with a polling loop, which will invoke an action when we have "currentBatchSize" number of messages.

# Complete plan
# Main loop will create an object for each message queue we need to monitor and trigger. 
#   Main object will spawn some threads. 
#       Main object will be intimated if there aren't any active threads, in RR fashion, it will choose a thread to poll.
#           Currently active thread will poll the message queue and wait until the "currentBatchSize" number of messages have arrived. 
#           Once we have enough messages, we send it to the action is json or whatever format it can accept and marks itself as inactive.
#       Main object maintains the rate at which threads are switching active status. 
#       If the threads are switching too often, it will first try to spawn more threads, until we reach max-threads-per-queue. 
#       If after spawning max threads, threads are still too big, it will increase the batch-size, until we reach max-batch-size.
#       If we reach both max-threads and max-batch-size, we will alternatively increase both of them.

# Initially will use command line args, will later use a file/open a socket. 
def main(argv):
    numArgs = 11
    if(len(argv)<numArgs):
        print ("\t Usage:  <action-name> <message-queue-name> <key-string> <maxNumInvocations> <batchSize> <outputDir> <parititonToMonitor> <launchType> <btwLaunchSleep> <numReqsPerIter> <verbose>\n")
        sys.exit()

    actionName = argv[1]
    messageQueue = argv[2]
    keyValue = argv[3]
    maxNumInvocations = int(argv[4].strip())
    batchSize = int(argv[5].strip())
    outputDir = argv[6].strip()
    parititonToMonitor = int(argv[7].strip())
    launchType = argv[8].strip()
    btwLaunchSleep = float(argv[9].strip())
    numReqsPerIter = int(argv[10].strip())

    verbose = False
    if(int(argv[10])>0):
        verbose = True
    
    pQ = pollQueue(actionName,messageQueue,keyValue,maxNumInvocations,batchSize,outputDir,parititonToMonitor,launchType,btwLaunchSleep,numReqsPerIter,verbose)


if __name__ == '__main__':
    main(sys.argv)
