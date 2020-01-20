#!/usr/bin/env bash

export NODE_OPTIONS=--max_old_space_size=8192

cd disclosure-deploy-repository
echo Installing dependancies
npm i --only=production serverless-latest-layer-version@2.0.0
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;
