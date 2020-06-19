#!/bin/bash

pushd ..
mvn -Dmaven.test.skip package
popd

mkdir -p tmp
cp -r ../replayer ./tmp
cp ../replayer.sh ./tmp
cp -r ../trace-generator ./tmp
cp ../generator.sh ./tmp
cp -r ../trace-transformer ./tmp
cp ../trace-transformer.sh ./tmp

ls | grep -v ^tmp$ | xargs -I a cp -r a ./tmp
docker build -t infsec/benchmark ./tmp

rm -r tmp
