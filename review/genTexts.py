#! /usr/bin/python
import sys,subprocess,random

outputDir = sys.argv[1]

numFiles = 1000
ipBaseTextFile = open(str(outputDir)+'/ip_textgen.log')
liBase = ipBaseTextFile.readlines()[0] #"Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature,     discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of 'de Finibus Bonorum et Malorum' (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, Lorem ipsum dolor sit amet.., comes from a line in section 1.10.32.The standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from de Finibus Bonorum et Malorum by Cicero are also reproduced in their exact original form, accompa"    
baseLen = len(liBase)
reviewLen = 1000

for fileIdx in range(numFiles):
    startIdx = random.randrange(0,baseLen-reviewLen)
    endIdx = startIdx+reviewLen #random.randrange(startIdx,baseLen)
    
    inputToRate = liBase[startIdx:endIdx] #"The ambiance is magical. The food and service was nice! The lobster and cheese was to die for and our steaks were cooked perfectly."
    
    reviewName = "r_"+str(fileIdx)
    opFilename = str(outputDir)+"/"+str(reviewName)+".log"
    opFile = open(opFilename,'w')
    opFile.write(inputToRate+"\n")
    opFile.close()

    pushToCephCmd = "../manageRados.py lrReviewServing putFile "+str(reviewName)+" "+str(opFilename)
    pushOp = subprocess.check_output(pushToCephCmd,shell=True,universal_newlines=True)

    #pushOp = "".join(tempPushOp.split("\n"))

    if(fileIdx%25==0): print ("\t startIdx: %d endIdx: %d opFilename: %s pushOp: %s "%(startIdx,endIdx,opFilename,pushOp))
    


    
