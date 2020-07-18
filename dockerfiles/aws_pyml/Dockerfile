FROM openwhisk/python3action
# lapack-dev is available in community repo.
#RUN echo "http://dl-4.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories

#RUN apt update
#RUN apt-get install software-properties-common -y
#RUN add-apt-repository ppa:deadsnakes/ppa -y
#RUN apt update
#RUN apt-get install -y python3.5
#RUN apt-get install -y python3-pip

#RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.5 35
#RUN update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 3
#RUN update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 30
#RUN update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 36


# add python packages
RUN pip install numpy
RUN pip install boto3

# lapack-dev is available in community repo.
RUN echo "http://dl-4.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories

# add package build dependencies
RUN apk add --no-cache \
        g++ \
        lapack-dev \
        gfortran \
        perl

RUN apk add make

RUN wget https://github.com/xianyi/OpenBLAS/archive/v0.3.6.tar.gz \
    && tar -xf v0.3.6.tar.gz \
    && cd OpenBLAS-0.3.6/ \
    && make BINARY=64 FC=$(which gfortran) USE_THREAD=1 \
    && make PREFIX=/usr/lib/openblas install

RUN ATLAS=/usr/lib/openblas/lib/libopenblas.so LAPACK=/usr/lib/openblas/lib/libopenblas.so pip install scipy==1.3


RUN pip install Cython
RUN pip install sklearn
RUN pip install pandas
