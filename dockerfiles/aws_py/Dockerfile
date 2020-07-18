FROM openwhisk/python3action

# lapack-dev is available in community repo.
RUN echo "http://dl-4.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories

# add package build dependencies
RUN apk add --no-cache \
        g++ \
        lapack-dev \
        gfortran

RUN pip install boto3
RUN pip install redis
RUN pip install redis-py-cluster
