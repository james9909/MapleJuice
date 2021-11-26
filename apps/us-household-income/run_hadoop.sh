#!/bin/bash

hdfs dfs -rm -r /user/hduser/us-income
hdfs dfs -copyFromLocal data /user/hduser/us-income
hdfs dfs -rm -r /user/hduser/us-income-output
time {
    hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar -file hadoop/map1.py -mapper hadoop/map1.py -file hadoop/reduce1.py -reducer hadoop/reduce1.py -input /user/hduser/us-income/* -output /user/hduser/us-income-output
}
