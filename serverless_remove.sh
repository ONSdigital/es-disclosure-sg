#!/usr/bin/env bash

cd disclosure-repository
echo Installing dependancies
serverless plugin install --name serverless-pseudo-parameters
serverless plugin install --name serverless-latest-layer-version
echo Destroying serverless bundle...
serverless remove --verbose;
