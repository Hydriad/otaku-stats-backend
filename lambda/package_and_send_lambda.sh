#!/bin/bash

pip3 install -r $1/requirements.txt --target=$1/deps/
cd $1/deps
zip -r9 ../../package.zip *
cd ..
zip -r9 ../package.zip *.py
cd ..

aws lambda update-function-code --function-name $2 --zip-file fileb://package.zip --profile cewdyn

rm -rf $1/deps/*
rm package.zip
