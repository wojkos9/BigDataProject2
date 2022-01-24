#!/bin/sh

name="$1"
[ "$name" = "ProjectTemplate" ] && echo "Cannot be $name" && exit 1

[ -d "$name" ] && echo "project exists" && exit 1

scdir="$name/src/main/scala"
main="$scdir/LoadData.scala"

cp -r ProjectTemplate "$name"

sed -i "s/_PROJECT_TEMPLATE_/$name/g" "$main" "$name/build.sbt"