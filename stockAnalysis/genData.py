#! /usr/bin/python

import rediscluster,json,random,sys,time
from rediscluster import RedisCluster

rangeLen = 25
maxMovement = 5

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

def genData(stockName,startDate,numVals):
    startDay = int(startDate.split('/')[1].strip())
    startMonth = int(startDate.split('/')[0].strip())
    startYear = int(startDate.split('/')[2].strip())

    curDay = startDay
    curMonth = startMonth
    curYear = startYear

    curVal = random.randrange(100,1000)
    curDate = str(curDay)+'/'+str(curMonth)+'/'+str(curYear)
    stockVals = []
    stockVals.append([curDate,curVal])
    minVal = 50
    for curIter in range(numVals-1):
        if(random.random()>0.75):
            tempVal = ( ( (random.randrange(0,maxMovement*2)/100)+0.01 ) * curVal)
            curVal = curVal - tempVal
        else:
            tempVal = ( ( (random.randrange(0,maxMovement//2)/100)+0.01 ) * curVal)
            curVal = curVal + tempVal

        if(curVal<minVal):
            curVal = minVal*(1+random.random())

        curDay,curMonth,curYear = incrementDate(curDay,curMonth,curYear,1)
        # tempDay = curDay + 1 
        # if(tempDay>numDaysMonths[curMonth]):
        #     curDay = 1
        #     tempMonth = curMonth+1
        #     if(tempMonth>len(numDaysMonths)):
        #         curMonth = 1
        #         curYear = curYear+1
        #     else:
        #         curMonth = tempMonth
        # else:
        #     curDay = tempDay

        curDate = str(curMonth)+'/'+str(curDay)+'/'+str(curYear)
        curArr = [curDate,curVal]
        stockVals.append(curArr)
        if(curIter%250==0): print ("\t curIter: %d curArr: %s curVal: %.3f "%(curIter,curArr,curVal))

    return stockVals

def pushData(rc,stockName,stockVals):
    usedKey = "None"
    numRanges = len(stockVals)//rangeLen

    for curRangeIdx in range(numRanges):
        startIdx = curRangeIdx*rangeLen
        rangeStartDate = stockVals[startIdx][0]
        vals = ""

        for idx in range(rangeLen):
            curDate,curVal = stockVals[startIdx+idx]
            vals = str(vals)+str(curDate)+","+str('%.3f'%(curVal))+"\n"

        curKey = str(stockName)+"_"+str(rangeStartDate)
        #if(curRangeIdx%10==0): 
        print ("\t curRangeIdx: %d rangeStartDate: %s key: %s vals: %s "%(curRangeIdx,rangeStartDate,curKey,vals[0:12]))
        rc.set(curKey,vals)

def getData(rc,stockName,startDate,numDays):

    numRanges = numDays//rangeLen
    rangeStartDate  = startDate
    allData = []

    startDay = int(startDate.split('/')[1].strip())
    startMonth = int(startDate.split('/')[0].strip())
    startYear = int(startDate.split('/')[2].strip())

    curDay = startDay
    curMonth = startMonth
    curYear = startYear

    for curRangeIdx in range(numRanges):
        curKey = str(stockName)+"_"+str(rangeStartDate)
        print ("\t curRangeIdx: %s curKey: %s "%(curRangeIdx,curKey))
        try:
            curRangeData = rc.get(curKey)
            splitData = curRangeData.strip().split('\n')
            for curLine in splitData:
                curLineData = curLine.strip().split(',')
                print ("\t curLineData: %s "%(curLineData))

        except KeyError as e:
            print ("\t curKey: %s %(curKey")

        curDay,curMonth,curYear = incrementDate(curDay,curMonth,curYear,rangeLen)
        print ("\t curDay: %d curMonth: %d curYear: %d "%(curDay,curMonth,curYear))
        rangeStartDate = str(curMonth)+'/'+str(curDay)+'/'+str(curYear)

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

    return curDay,curMonth,curYear
    
def main(args):
    hosts = [{"host": "172.24.72.27", "port": "7000"}, {"host": "172.24.198.99", "port": "7001"}, {"host": "172.24.118.202", "port": "7002"}]
    hosts = [{"host": "172.31.54.77", "port": "7000"}, {"host": "172.31.57.25", "port": "7001"}, {"host": "172.31.63.168", "port": "7002"}]
    #hostname = "172.24.202.10";portnum = "7000"
    #hosts = [{"host": "172.31.82.52", "port": "7000"}, {"host": "172.31.89.207", "port": "7001"}, {"host": "172.31.92.44", "port": "7002"}]
    allStockVals = []
    allStocks = ["INTC","GOOG","AAPL","AMZN","MSFT","VMW"]
    numVals = 10000

    #calcStartDate("1/1/2000")
    #sys.exit()
    for curStock in allStocks:
        tempStockVal = genData(curStock,"1/1/1998",numVals); 
        allStockVals.append([curStock,tempStockVal])

    startup_nodes = [hosts[random.randint(0,2)]]
    #rc = rediscluster.StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    fooVal = rc.get("foo")

    minDays = min(numDaysMonths.values())
    if(rangeLen>minDays):
        print ("\t Error minDays: %d is smaller than rangeLen: %d, will break the increment function..")
        sys.exit()

    for curStock,curStockVals in allStockVals:
        stockStartDate = curStockVals[0][0]
        print ("\t curStock: %s stockStartDate: %s "%(curStock,stockStartDate))
        pushData(rc,curStock,curStockVals)
        time.sleep(1)
        getData(rc,curStock,stockStartDate,20)


if __name__ == "__main__":
    main(sys.argv)
    
