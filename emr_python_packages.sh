#!/bin/bash

sudo yum -y install at

sudo python3 -m pip uninstall -y numpy && \
sudo python3 -m pip install \
    keybert==0.5.0 \
    nltk==3.6.7 \
    sentence-transformers==2.1.0 \
    warcio==1.7.4 \
    boto3==1.20.46 \
    bs4==0.0.1 \
    readable-content \
    | at now +3 minutes
