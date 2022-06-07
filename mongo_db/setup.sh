#!/bin/bash

aws s3 cp s3://playgroud/source.zip source.zip
unzip source.zip -d source

cd source
poetry env use $(which -a python3.8)
poetry install