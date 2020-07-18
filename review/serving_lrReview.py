#! /usr/bin/python3

import json,subprocess,sys,re,numpy,random
import scipy.sparse as sp

import pandas as pd
from time import time

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.externals import joblib
import boto3

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

cleanup_re = re.compile('[^a-z]+')
def cleanup(sentence):
    sentence = sentence.lower()
    sentence = cleanup_re.sub(' ', sentence).strip()
    return sentence

def main(args):

    #name = "stranger" 
    name = args.get("params", "stranger")
    liBase = "Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature,     discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of 'de Finibus Bonorum et Malorum' (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, Lorem ipsum dolor sit amet.., comes from a line in section 1.10.32.The standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from de Finibus Bonorum et Malorum by Cicero are also reproduced in their exact original form, accompa"    

    totalNumIpFiles = 1000

    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)

        bucketName = msg["bucketName"] 
        modelKeyName = msg["modelKeyName"]
        idfKeyName = msg["idfKeyName"] 
        vocabKeyName = msg["vocabKeyName"] 
        inputToRate = msg["inputToRate"] 

        numFiles = bucketName["numFiles"]
        startFile =  bucketName["startFile"]
        endFile = bucketName["endFile"]

    else:
        bucketName = "lr-review-data"
        modelKeyName = "lr_model.p0"
        idfKeyName = "idf.p0" 
        vocabKeyName = "vocab.p0"
        
        numFiles = 6
        startFile = random.randrange(0,totalNumIpFiles-numFiles-1)
        endFile = startFile+numFiles


    if(endFile<startFile):
        endFile = startFile+1
    if(startFile>=(totalNumIpFiles-1)):
        startFile = totalNumIpFiles-2
        endFile = totalNumIpFiles-1

    #return "Error: params not found!! name -->"+str(name)
    bucketStr = "beginning.."
    bucketStr = str(bucketStr)+"\t "+str(idfKeyName)
    localIdfFile = "temp.csv"
    print ("\t bucketStr: %s "%(bucketStr))
    pullFileFromStorage(bucketName,idfKeyName,localIdfFile)


    bucketStr = str(bucketStr)+"\t "+str(vocabKeyName)
    localVocabFile = "temp.json"
    print ("\t bucketStr: %s "%(bucketStr))
    pullFileFromStorage(bucketName,vocabKeyName,localVocabFile)

    bucketStr = str(bucketStr)+"\t "+str(modelKeyName)
    localModel = "temp_model.pk"
    print ("\t bucketStr: %s "%(bucketStr))
    pullFileFromStorage(bucketName,modelKeyName,localModel)

    start = time()
    allYs = []
    for idx in range(startFile,endFile):

        curReviewKeyName = "r_"+str(idx)+".log"
        localFile = "temp.log"
        print ("\t idx: %d review: %s "%(idx,curReviewKeyName)) 
        pullFileFromStorage(bucketName,curReviewKeyName,localFile)

        allLines = open(localFile).readlines()
        inputToRate = ""
        if(len(allLines)>0):
            inputToRate = allLines[0]
        else:
            print ("\t ERROR! reviewFile: %s is empty! "%(curReviewKeyName))
            continue

        df_input = pd.DataFrame()
        df_input['x'] = [inputToRate]
        df_input['x'] = df_input['x'].apply(cleanup)
        
        readIdfs = numpy.genfromtxt(localIdfFile,delimiter=",")
        vocabulary = json.load(open(localVocabFile, mode = 'r'))

        # subclass TfidfVectorizer
        class MyVectorizer(TfidfVectorizer):
            # plug our pre-computed IDFs
            #TfidfVectorizer.idf_ = readIdfs
            #TfidfVectorizer.idf_ = idfs
            def putIdfs(self,readIdfs):
                print ("\t Updating idfs..")
                TfidfVectorizer.idf_ = readIdfs

        # instantiate vectorizer
        newVect = MyVectorizer(lowercase = False,
                                  min_df = 2,
                                  norm = 'l2',
                                  smooth_idf = True)
        newVect.putIdfs(readIdfs)
        # plug _tfidf._idf_diag
        newVect._tfidf._idf_diag = sp.spdiags(readIdfs,
                                             diags = 0,
                                             m = len(readIdfs),
                                             n = len(readIdfs))  

        print ("\t len(newVect.idf_): %d len(vocabulary): %d "%(len(newVect.idf_),len(vocabulary)))

        newVect.vocabulary_ = vocabulary
        X = newVect.transform(df_input['x'])
        model = joblib.load(localModel)
        y = model.predict(X)    
        allYs.append(str(y))

    latency = time() - start
    appendedResults = "".join(allYs)

    resStr = "y: "+str(y)+"\nlatency: "+str('%.3f'%(latency))+"\n bucketStr "+str(bucketStr)+""
    print ("\t len(inputToRate): %s y: %s latency: %s resStr: %s "%(len(inputToRate),y,latency,resStr))
    return {"greeting:":resStr}

if __name__ == "__main__":
    main(sys.argv)
