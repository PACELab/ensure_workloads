#! /usr/bin/python3

import json,subprocess,sys,re,numpy
import scipy.sparse as sp
import boto
import boto.s3.connection

import pandas as pd
from time import time

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.externals import joblib

cleanup_re = re.compile('[^a-z]+')
def cleanup(sentence):
    sentence = sentence.lower()
    sentence = cleanup_re.sub(' ', sentence).strip()
    return sentence

def main(args):

    name = "stranger" 
    #name = args.get("params", "stranger")
    if(name != "stranger"):
        messagesArr = json.dumps(name);
        msg = json.loads(messagesArr)

        bucketName = msg["bucketName"] 
        modelKeyName = msg["modelKeyName"]
        idfKeyName = msg["idfKeyName"] 
        inputToRate = msg["inputToRate"] 
    else:
        bucketName = "lr_review_data"
        modelKeyName = "lr_model.p0"
        idfKeyName = "idf.p0" 
        vocabKeyName = "vocab.p0"
        inputToRate = "The ambiance is magical. The food and service was nice! The lobster and cheese was to die for and our steaks were cooked perfectly."

        #return "Error: params not found!! name -->"+str(name)

    userDetails = {
        "user": "lrReview_serving",
        "access_key": "93U5ZBFLJYX9HQ3RM6U0",
        "secret_key": "fAO3N0P0LOk6cDYQeAcR4SCtGkGnUJEAav1HvgwI",
        "host" : "172.24.202.7", #bay16
        "port" : 7480            
    }

    access_key = userDetails["access_key"]
    secret_key = userDetails["secret_key"]
    hostname = userDetails["host"]
    portNum = userDetails["port"]

    bucketStr = hostname
    conn = boto.connect_s3(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            host = hostname,
            port = portNum,
            is_secure=False,               # uncomment if you are not using ssl
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )

    bucketStr = str(hostname)+":"+str(portNum)+"\t"    
    # Check whether bucket exists
    try:
        curBucket = conn.get_bucket(bucketName)
    except boto.exception.S3ResponseError:
        return {"greeting":"Error-Failing at fetching bucket. bucketStr--> "+str(bucketStr)}

    bucketStr = str(bucketStr)+"\t "+str(idfKeyName)
    curIdfKey = curBucket.get_key(idfKeyName)
    localIdfFile = "temp.csv"
    print ("\t bucketStr: %s "%(bucketStr))
    try:
        curIdfObj = curIdfKey.get_contents_to_filename(localIdfFile)
    except AttributeError:
        return {"greeting":"Error-Failing at fetching input-image. bucketStr--> "+str(bucketStr)}    

    bucketStr = str(bucketStr)+"\t "+str(vocabKeyName)
    curVocabKey = curBucket.get_key(vocabKeyName)
    localVocabFile = "temp.json"
    print ("\t bucketStr: %s "%(bucketStr))
    try:
        curVocabObj = curVocabKey.get_contents_to_filename(localVocabFile)
    except AttributeError:
        return {"greeting":"Error-Failing at fetching input-image. bucketStr--> "+str(bucketStr)}    

    bucketStr = str(bucketStr)+"\t "+str(modelKeyName)
    curModelKey = curBucket.get_key(modelKeyName)
    localModel = "temp_model.pk"
    print ("\t bucketStr: %s "%(bucketStr))
    try:
        curModelObj = curModelKey.get_contents_to_filename(localModel)
    except AttributeError:
        return {"greeting":"Error-Failing at fetching model. bucketStr--> "+str(bucketStr)}    

    #localIdfFile = "/home/amoghli13/PACE/serverless/other_workloads/serverless-faas-workbench/dataset/amzn_fine_food_reviews/comb_intermed.csv"
    #localModel = "/home/amoghli13/PACE/serverless/other_workloads/serverless-faas-workbench/dataset/model/lr_model.pk"

    start = time()

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

    latency = time() - start
    #print ("\t y: %s latency: %s "%(y,latency))
    
    resStr = "y: "+str(y)+"\nlatency: "+str('%.3f'%(latency))+"\n bucketStr "+str(bucketStr)
    return {"greeting:":resStr}

if __name__ == "__main__":
    main(sys.argv)
