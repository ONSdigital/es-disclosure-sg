#!/usr/bin/env bash

cd disclosure-deploy-repository
echo Installing dependancies
npm i serverless-latest-layer-version
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;