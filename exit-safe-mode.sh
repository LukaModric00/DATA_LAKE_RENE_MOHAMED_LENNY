#!/bin/bash

sleep 10

echo "Sortie du Safe Mode..."
hdfs dfsadmin -safemode leave

hdfs dfsadmin -printTopology
