#! /usr/bin/python

import json,random,sys,time
from rediscluster import RedisCluster

rangeLen = 25

numDaysMonths = {
    1 : 31, 2 : 28, 3 : 31, 4 : 30, 5 : 31, 6 : 30,
    7 : 31, 8 : 31, 9 : 30, 10 : 31, 11 : 30, 12 : 31
}

cumulDaysMonths = {
    1 : 31, 2 : 59, 3 : 90, 4 : 120, 5 : 151, 6 : 181,
    7 : 212, 8 : 243, 9 : 273, 10 : 304, 11 : 334, 12 : 365
}

def incrementDate(curDay,curMonth,curYear,incrementVal):

    tempDay = curDay + incrementVal 
    if(tempDay>numDaysMonths[curMonth]):
        curDay = tempDay - numDaysMonths[curMonth]
        tempMonth = curMonth+1
        if(tempMonth>len(numDaysMonths)):
            curMonth = 1
            curYear = curYear+1
        else:
            curMonth = tempMonth
    else:
        curDay = tempDay

    return curDay,curMonth,curYear

def calcStartDate(curStartDate):
    universalStartDate = "1/1/1998"  
    minNumRangesInYear = 365//rangeLen

    usMonth = int(universalStartDate.split('/')[0].strip())
    usDay = int(universalStartDate.split('/')[1].strip())
    usYear = int(universalStartDate.split('/')[2].strip())

    startDay = int(curStartDate.split('/')[1].strip())
    startMonth = int(curStartDate.split('/')[0].strip())
    startYear = int(curStartDate.split('/')[2].strip())

    diffYear = startYear - usYear
    deltaDays = 0 
    if(diffYear>0):
        deltaDays = (diffYear)*365

    diffMonth = startMonth - usMonth 
    if(diffMonth>0):
        deltaDays = deltaDays+cumulDaysMonths[diffMonth]

    diffDay = startDay - usDay 
    if(diffDay>0):
        deltaDays = deltaDays+diffDay

    rangeStartIdx = deltaDays//rangeLen
    diffIdx = rangeStartIdx; 
    print ("\t rangeStartIdx: %d deltaDays: %d diffYear: %d diffMonth: %d diffDay: %d "%(rangeStartIdx,deltaDays,diffYear,diffMonth,diffDay))
    
    # WARNING: Have baked the delta based on rangeLen=25!!
    yearEndDate = ""
    if(diffIdx<minNumRangesInYear):
        yearEndDate = "1/1/1998"
    else:
        numYearsOffset = 0; 
        while(diffIdx>minNumRangesInYear):
            if(numYearsOffset%2==0):
                diffIdx = diffIdx - minNumRangesInYear
            else:
                diffIdx = diffIdx - (minNumRangesInYear+1)
            numYearsOffset+=1
            print ("\t diffIdx: %d numYearsOffset: %d "%(diffIdx,numYearsOffset))

        offsetIdx = rangeStartIdx-diffIdx
        offsetYearsInDays = (rangeStartIdx-diffIdx)*rangeLen 
        subtractDays = (numYearsOffset*365) - offsetYearsInDays

        yearEndDay = 31-subtractDays+1
        if(yearEndDay<0):
            yearEndDate = "1/1/1998"
            print ("\t ERROR in logic, going to default date of ")
            print ("\t offsetIdx: %d offsetYearsInDays: %d subtractDays: %d "%(offsetIdx,offsetYearsInDays,subtractDays))
            #diffIdx = 0
            yearEndDate = "12/"+str(yearEndDay)+"/"+str(usYear+numYearsOffset-1)
        else:  
            yearEndDate = "12/"+str(yearEndDay)+"/"+str(usYear+numYearsOffset-1)
            print ("\t offsetIdx: %d offsetYearsInDays: %d subtractDays: %d "%(offsetIdx,offsetYearsInDays,subtractDays))

    print ("\t yearEndDate: %s  "%(yearEndDate))
    curDay = int(yearEndDate.split('/')[1].strip())
    curMonth = int(yearEndDate.split('/')[0].strip())
    curYear = int(yearEndDate.split('/')[2].strip())

    while(diffIdx>0):
        curDay,curMonth,curYear = incrementDate(curDay,curMonth,curYear,rangeLen)
        print ("\t diffIdx: %d curDay: %d curMonth: %d curYear: %d "%(diffIdx,curDay,curMonth,curYear))        
        diffIdx-=1

    toUseStartDate = str(curMonth)+"/"+str(curDay)+"/"+str(curYear)
    #return curDay,curMonth,curYear
    return toUseStartDate

def getData(rc,stockName,startDate,numDays):
    numRanges = ((numDays-1)//rangeLen)+1
    rangeStartDate  = startDate
    allData = []

    startDay = int(startDate.split('/')[1].strip())
    startMonth = int(startDate.split('/')[0].strip())
    startYear = int(startDate.split('/')[2].strip())

    curDay = startDay
    curMonth = startMonth
    curYear = startYear
    allResIdx = {}
    allResArr = []

    for curRangeIdx in range(numRanges):
        curKey = str(stockName)+"_"+str(rangeStartDate)
        if(curRangeIdx%50==0): print ("\t curRangeIdx: %s curKey: %s "%(curRangeIdx,curKey))
        try:
            curRangeData = rc.get(curKey)
            splitData = curRangeData.strip().split('\n')
            for curLine in splitData:
                curLineData = curLine.strip().split(',')
                #print ("\t curLineData: %s "%(curLineData))
                curDate = str(curLineData[0].strip())
                stockVal = float(curLineData[1].strip())

                allResIdx[curDate] = len(allResArr)-1 #allRes[curLineData[1]]
                allResArr.append([curDate,stockVal])

        except KeyError as e:
            print ("\t curKey: %s %(curKey")

        curDay,curMonth,curYear = incrementDate(curDay,curMonth,curYear,rangeLen)
        if(curRangeIdx%50==0): print ("\t curDay: %d curMonth: %d curYear: %d "%(curDay,curMonth,curYear))
        #rangeStartDate = str(curDay)+'/'+str(curMonth)+'/'+str(curYear)
        rangeStartDate = str(curMonth)+'/'+str(curDay)+'/'+str(curYear)
        
    return allResIdx,allResArr

def analyseStockData(allStockData):
    cumulSum = 0
    count = 0
    avgVal = []

    for curDate,curVal in allStockData:
        cumulSum+=curVal
        count+=1

        curAvg = cumulSum/count
        avgVal.append('%.3f'%(curAvg))
        #print ("\t curDate: %s curVal: %.3f curAvg: %.3f cumulSum: %.3f count: %d "%(curDate,curVal,curAvg,cumulSum,count))
    return avgVal

def main(args):
    start = time.time()
    name = args.get("params", "stranger")
    #name = "stranger"
    progressStr = "stranger"
    needDefVals = 0
    # if(name != "stranger"):
    #     messagesArr = json.dumps(name);
    #     msg = json.loads(messagesArr)
    #     len_msg = len(messagesArr)
    #     value = " This is the default value msg"

    #     numMessages = 0
    #     try:
    #         stockName = msg["stockName"]
    #         startDate = msg["startDate"]
    #         numDays = msg["numDays"]

    #         needDefVals = 0                
    #     except (KeyError,IndexError):
    #         needDefVals = 1

    # else:
    #     needDefVals =1 

    # if(needDefVals):
    #     stockName = "INTC"
    #     startDate = "8/8/1998"
    #     numDays = 75
    print ("\t name: %s args: %s "%(name,args))
    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)

        print ("\t msg: %s "%(str(msg)))
        stockName = msg["stockName"]
        startDate = msg["startDate"]
        numDays = msg["numDays"]
    else:
        stockName = "INTC"
        month = random.randint(1,12)
        day = random.randint(1,25)
        year = random.randint(0,4) + 1998
        startDate = str(month)+"/"+str(day)+"/"+str(year)  #"8/8/1998"
        #startDate = "11/22/2013"
        numDays = 5000

    progressStr = str(stockName)+" "+str(startDate)+" "+str(numDays)
    print ("\t startDate: %s "%(startDate))
    hosts = [{"host": redis-host-1, "port": "7000"}, {"host": redis-host-2, "port": "7001"}, {"host": redis-host-3, "port": "7002"}]

    startup_nodes = [hosts[random.randint(0,2)]]
    #rc = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    tempStocks = ["INTC","GOOG","AAPL","AMZN","MSFT","VMW"]
    numStocks = len(tempStocks)
    allStocks = []
    for curStock in range(numStocks):
        idx = random.randrange(0,len(tempStocks))
        curStock = tempStocks[idx]
        allStocks.append(curStock)
        tempStocks.remove(curStock)

    toUseStartDate = calcStartDate(startDate)
    for curStock in allStocks:
        allResIdx,allResArr = getData(rc,curStock,toUseStartDate,numDays)
        avgVals = analyseStockData(allResArr)

    resStr = ""
    numResDays = min(numDays,100)
    resStr = avgVals[0]
    for i in range(numResDays):
        resStr = str(resStr)+","+str(avgVals[i])

    avgKey = "avg_"+str(curStock)+"_startDate"+str(startDate)+"_"+str(numDays)
    rc.set(avgKey,resStr)

    end = time.time()
    totalTime = end-start
    progressStr = str(progressStr)+"\t time "+str('%.3f'%(totalTime))+"\t end "+str('%.3f'%(end))+"\t start "+str('%.3f'%(start))

    print ("\t Time: %.3f end: %.3f start: %.3f avgKey: %s \n resStr: %s "%(totalTime,end,start,avgKey,resStr))
    return {"result":resStr}


if __name__ == "__main__":
    main(sys.argv)

