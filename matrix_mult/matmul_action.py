import numpy as np
import json,time,random
import boto3

aws_access_id = "TBD"
aws_secret_id = "TBD" #os.getenv('AWS_SECRET_ID',"")

def matmul(X, Y, rows, colums):
    result = np.zeros(shape=(rows, colums))
    # result = [[0]*colums]*rows
    for f in range(len(X)):
        for g in range(len(Y[0])):
            for c in range(len(Y)):
                result[f][g] += X[f][c] * Y[c][g]
    return result

def matMul2(matA, matB, Adim0, Adim1, Bdim1):
    # Should check the dimensions, etc..
    matC = []
    # print ("\t len(matA): %d len(matB): %d "%(len(matA),len(matB)))
    Cstr = ""
    for xIdx in range(Adim0):  # len(matA)):
        curRow = [];
        tempStr = ""
        for yIdx in range(Bdim1):  # len(matB)):
            accum = 0
            # print ("\t xIdx: %s matA[xIdx]: %s matA[xIdx][3]: %d "%(xIdx,matA[xIdx],matA[xIdx][3]))
            # print ("\t xIdx: %d yIdx: %d matA[xIdx]: %s matB[yIdx]: %s "%(xIdx,yIdx,matA[xIdx],matB[yIdx]))
            for runningIdx in range(Adim1):  # len(matA)):
                accum = accum + (matA[xIdx][runningIdx] * matB[runningIdx][yIdx])
                # print ("\t runningIdx: %d accum: %d matA[xIdx][runningIdx]: %d matB[runningIdx][yIdx]: %d "%(runningIdx,accum,matA[xIdx][runningIdx],matB[runningIdx][yIdx]))
            curRow.append(accum)

            if (yIdx != 0):
                tempStr = str(tempStr) + "," + str(accum)
            else:
                tempStr = str(accum)

        matC.append(curRow)
        if (xIdx != 0):
            Cstr = str(Cstr) + "\n" + str(tempStr)
        else:
            Cstr = str(tempStr)

    return matC,Cstr

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

def readObject(params,args):
    name = args.get("params","invalid")
    if(name!="invalid"):
        paramsArray = json.dumps(name)
        msg = json.loads(paramsArray)

        params["bucketName"] = msg['bucketName']
        params["A_list"] = msg['A_list']
        params["B_list"] = msg['B_list']
        params["size"] = msg['size'] #matlen = msg['matlen'] #matrix = msg['matrix']
        params["outlen"] = msg['outlen']
        params["outmatrix"] = msg['outmatrix']   
        params["repeat"] = msg['repeat']   
    else:
        #"bucketName": "matrixmultiplication", "A_list": "trail512.p0", "B_list": "trail512.p0", "size": "512", "outmatrix": "newC","outlen":512

        params["bucketName"] = "ow-aws-mm" #msg['bucketName']
        params["A_list"] = "trial_512.txt" 
        params["B_list"] = "trial_512.txt"
        params["size"] = 256 
        params["outlen"] = 256
        params["outmatrix"] = "newC" 
        params["repeat"] = 4

        #return "Error: params not found!! name -->"+str(name)
    return "Alright"


def main(args):

    params = {}
    res = readObject(params, args)

    bucketName = params.get('bucketName', '')
    A_list = params.get('A_list', '')
    B_list = params.get('B_list', '')
    size = params.get('size', '')
    outlen = params.get('outlen','')
    outmatrix = params.get('outmatrix', '')
    repeat = params.get('repeat',1)

    print ("\t bucketName: %s A_list: %s B_list: %s outlen: %s size: %s outmatrix : %s "%(bucketName,A_list,B_list,outlen,size,outmatrix))

    compTime = 0.0; pushTime = 0.0
    for curIter in range(repeat):
        tempAFilename = "tempA.log"; tempBFilename = "tempB.log"    

        pullFileFromStorage(bucketName,A_list,tempAFilename)
        pullFileFromStorage(bucketName,B_list,tempBFilename)

        matA = []; tempAFile = open(tempAFilename).readlines(); 
        for curLine in tempAFile:
            curRowStr = curLine.strip().split(",")
            curRow = [int(elmt) for elmt in curRowStr]
            matA.append(curRow)

        matB = [] ; tempBFile = open(tempBFilename).readlines()
        for curLine in tempBFile:
            curRowStr = curLine.strip().split(",")
            curRow = [int(elmt) for elmt in curRowStr]
            matB.append(curRow)

        start = time.time()
        Adim0 = int(size); Adim1 = int(size); Bdim1 = int(size)
        matC,Cstr = matMul2(matA, matB, Adim0, Adim1, Bdim1)

        opWriteStart = time.time()
        #opFilename = "out_"+str(A_list[1:]) + '_' + str(B_list[1:])+'_'+str(size)+".txt"
        for subIterIdx in range(repeat):
            mat1Idx = random.randrange(1,10000)
            mat2Idx = random.randrange(1,10000)
            opFilename = "out_"+str(mat1Idx) + '_' + str(mat2Idx)+'_'+str(size)+".txt"
            opFile = open(opFilename,'w')
            opFile.write(Cstr+"\n")
            opFile.close()

            outKeyname = opFilename
            pushFileToStorage(bucketName,outKeyname,opFilename)
        
        opWriteEnd = time.time()
        compTime+= (opWriteStart-start)
        pushTime+= (opWriteEnd- opWriteStart)
        print ("\t Iter: %d compTime: %.3f pushTime: %.3f "%(curIter,compTime,pushTime))

    return {"result": "SUCCESS compTime: %.3f pushTime: %.3f outKey: %s "%(compTime,pushTime,opFilename)}








