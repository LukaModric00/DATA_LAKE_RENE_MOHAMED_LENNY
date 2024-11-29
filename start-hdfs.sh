#!/bin/bash
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format
fi
hdfs namenode
