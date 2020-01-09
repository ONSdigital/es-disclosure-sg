#!/usr/bin/env bash

cd disclosure-repository
echo Destroying serverless bundle...
serverless remove --verbose;
