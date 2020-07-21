
This repo publishes the applications used in the paper [ENSURE: Efficient Scheduling and Autonomous
Resource Management in Serverless Environments] (https://www3.cs.stonybrook.edu/~anshul/acsos20_ensure.pdf). The solution proposed in the paper is available in  (https://github.com/pacelab/ensure)

Requirements for individual applications are as follows:

Application:            Requirement

* EmailGen                S3 credentials (variable aws_access_id, aws_secret_id)
* StockAnalysis           Redis credentials (variable redis-host-1 to 3)
* File encryption         S3 + redis credentials
* Sentiment Review        S3 credentials
* Sort                    S3 credentials
* Matrix multiplication   S3 credetials

* Image-resizing 		S3 credentials
* Real time analytics 	Redis + Kafka credentials
* CFD			S3 credentials
* Nearest neighbors 	S3 credentials

To run the applications, a functional OpenWhisk cluster is required, along with application specific requirements mentioned above. 
The run commands for the applications with default parameters are as follows

* wsk -i action update emailGen_v1 --docker pacelab/aws_py emailGen/emailGen.py 
* wsk -i action update stockAnalysis_v1 --docker pacelab/aws_py stockAnalysis/stockAnalysis.py
* wsk -i action update fileEncrypt_v1 fileEncrypt/db_fileEncrypt.py --docker pacelab/aws_file_encrypt -t 90000 # should rename it to fileEncrypt_v1
* wsk -i action update serving_lrReview_v1 review/serving_lrReview.py --docker pacelab/aws_pyml -t 300000 

* wsk -i action update sort_v1 --docker pacelab/aws_py  sorting/sort_v1.py -t 300000 -m 512
* wsk -i action update matmulaction_v2 --docker pacelab/aws_pyml   matrix_mult/matmul_action.py -t 300000 -m 512

* wsk -i action update euler3d_cpu_v1 --docker pacelab/aws_py ../rodinia_cfd/driverCfd/euler3d_cpu.py -t 90000 -m 512
* wsk -i action update realTimeAnalytics_v1 --docker pacelab/aws_py ../realTimeAnalytics/dockerBasedAction/realTimeAnalytics_v1.py
* wsk -i action update rodinia_nn_v1 --docker pacelab/aws_py ../rodinia_nn/driverNn/nn.py
* wsk -i action update imageResizing_v1 --docker pacelab/aws_pyml ../imageResizing/dockerBasedAction/imageResizing.py

All the actions/functions have default parameters and can be invoked without specifying input parameters. 


