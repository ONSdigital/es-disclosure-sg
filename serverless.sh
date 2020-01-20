#!/usr/bin/env bash

cd disclosure-deploy-repository
echo Installing dependancies
serverless plugin install --name serverless-latest-layer-version@2.0.0
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;