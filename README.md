
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
