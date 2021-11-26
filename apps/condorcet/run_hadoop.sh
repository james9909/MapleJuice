#!/bin/bash

hdfs dfs -rm -r /user/hduser/condorcet-output /user/hduser/condorcet-output-final
time {
    hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar -file hadoop/map1.py -mapper hadoop/map1.py -file hadoop/reduce1.py -reducer hadoop/reduce1.py -input /user/hduser/condorcet/* -output /user/hduser/condorcet-output
    hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar -file hadoop/map2.py -mapper hadoop/map2.py -file hadoop/reduce2.py -reducer hadoop/reduce2.py -input /user/hduser/condorcet-output* -output /user/hduser/condorcet-output-final
}
