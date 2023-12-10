#!/bin/env bash
set -xe

cd package
zip -r9 ../package.zip .
cd ..
zip package.zip lambda_function.py
