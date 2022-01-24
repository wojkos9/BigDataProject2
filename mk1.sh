#!/bin/bash

fun() {
    while read line; do
        if [ "$skip" = 1 ]; then
            echo "$line" | grep -q ")" && skip=0
        else
            if echo "$line" | grep -q "$1"; then echo "$line";
            elif echo "$line" | grep -q 'mkDeltaTable("'; then skip=1;
            else
                echo "$line"
            fi
        fi
    done < create-tables.scala
}

spark-shell --jars /opt/apache-zeppelin/local-repo/spark/delta-core_2.12-1.0.0.jar -i <( fun $1 )