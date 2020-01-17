#!/usr/bin/env bash

export NODE_OPTIONS=--max_old_space_size=8192

cd disclosure-deploy-repository
echo Installing dependancies
serverless plugin install --name serverless-latest-layer-version
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;
