#!/bin/sh

# (Optional) Upload local data to hdfs
# hadoop fs -mkdir -p labs
# hadoop fs -copyFromLocal us-accidents labs

deps="--packages io.delta:delta-core_2.12:1.0.0"
script_cmd="spark-shell $deps -i"
jar_cmd="spark-submit $deps --class LoadData"

run_jar() {
    echo "Running $1..."
    $jar_cmd "$1"
}

fail() {
    echo "Fail"
    exit 1
}

echo "Deleting and recreating tables..."
$script_cmd create-tables.scala || fail

echo "Loading data to tables..."
facts_jar=`ls jar/accidentwarehouse*.jar`
for jar in `ls jar/*.jar`; do
    if [ "$jar" != "$facts_jar" ]; then
        run_jar "$jar" || fail
    fi
done
run_jar "$facts_jar" || fail

echo "Done"
