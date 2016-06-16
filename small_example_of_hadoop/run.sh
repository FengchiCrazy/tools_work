#/bin/bash

hdfs_input="/user/test/log*"
hdfs_output="/user/result"

mapper_file="mapper.py"
mapper_cmd="python mapper.py"
reducer_file="reducer.py"
reducer_cmd="python reducer.py"

cmd="${HADOOP_HOME}/bin/hadoop"
opt="jar ${HADOOP_HOME}/hadoop-streaming-1.0.3.jar"
hfs="${HADOOP_HOME}/bin/hadoop fs"

if ${hfs} -test -d ${hdfs_output};then
   ${hfs} -rmr ${hdfs_output}
fi

${cmd} ${opt}       \
  -input ${hdfs_input}   \
  -output ${hdfs_output} \
  -mapper "${mapper_cmd}"  \
  -reducer "${reducer_cmd}" \
  -file ${mapper_file}   \
  -file ${reducer_file}  \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -jobconf mapred.map.tasks=200    \
  -jobconf mapred.reduce.tasks=13  \
  -jobconf mapred.job.name="WordCount"  \
  -jobconf stream.num.map.output.key.fields=1  \
  -jobconf num.key.fields.for.partition=1
  
