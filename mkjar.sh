#!/bin/sh
out="project/jar"
name="${1%/}"
echo $name
pushd "$name"
sbt package
popd

[ ! -d "$out" ] && mkdir "$out"
for jar in `ls "$name"/target/scala-*/*.jar`; do
    cp "$jar" "$out"
done