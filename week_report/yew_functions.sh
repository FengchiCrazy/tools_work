#!/bin/bash

function YWLOG()
{
    now_time=`date +%D_%H:%M:%S`
    log_message="$*"

    echo -e "$now_time" "$log_message"
}

function YWFLOG()
{
    YWLOG "\033[0;1;31mFATA\033[m" "$*"
}

function YWWLOG()
{
    YWLOG "\033[0;1;33mWARN\033[m" "$*"
}

function YWILOG()
{
    YWLOG "\033[0;1;32mINFO\033[m" "$*"
}

function YWDLOG()
{
    YWLOG "DEBUG" "$*"
}

HDP_COMB="-inputformat org.apache.hadoop.mapred.CombineFileInputFormat -jobconf mapred.max.split.size=2048000000"

function runhdp()
{
    # get vars
    local hdp_pool_name="${1}"; shift
    local hdp_name="${1}"; shift
    local hdp_input="${1}"; shift
    local hdp_output="${1}"; shift
    local mapper_file="${1}"; shift
    local reducer_file="${1}"; shift
    local hdp_mapper_cmd="${1}"; shift
    local hdp_reducer_cmd="${1}"; shift
    local hdp_map_cap="${1}"; shift
    local hdp_reduce_cap="${1}"; shift
    local hdp_reduce_task="${1}"; shift
    local hdp_adds="$*"; shift

    # check input
    YWILOG "NAME     [$hdp_name]"
    YWILOG "INPUT    [$hdp_input]"
    YWILOG "OUTPUT   [$hdp_output]"
    YWILOG "MAP      [$hdp_mapper_cmd]"
    YWILOG "REDUCE   [$hdp_reducer_cmd]"
    YWILOG "MAP_CAP  [$hdp_map_cap]"
    YWILOG "RED_CAP  [$hdp_reduce_cap]"
    YWILOG "RED_TASK [$hdp_reduce_task]"
    YWILOG "POOL_NAME[$hdp_pool_name]"

    hadoop fs -test -d ${hdp_output} &> /dev/null
    hdp_exist=$?
    if [[ "$hdp_exist" == "0" && "$MODE" != "RETRY" ]]; then
            YWWLOG "[$hdp_output] exists, and NOT in RETRY mode, skip this step"
    else
        if [[ "$hdp_exist" == "0" ]]; then
            YWWLOG "[$hdp_output] exists, in RETRY mode, rmr [$hdp_output]"
            hadoop fs -rmr ${hdp_output}
        fi 
        
        YWILOG "HADOOP RUN COMMAND:
        hadoop jar $HADOOP_STREAMING_HOME/$STREAMING_JAR 
        -D mapred.fairscheduler.pool=\"${hdp_pool_name}\"
        -D mapred.job.name=\"[ads_yew][${hdp_name}]\" 
        -D mapred.reduce.tasks=\"$hdp_reduce_task\" 
        -cacheArchive \"${PYTHON_LIB}/python27.tar.gz#python27\" 
        -input \"${hdp_input}\" 
        -output \"${hdp_output}\" 
        -file \"${mapper_file}\" 
        -file \"${reducer_file}\"
        -mapper \"${hdp_mapper_cmd}\" 
        -reducer \"${hdp_reducer_cmd}\" 
        ${hdp_adds}"

        hadoop jar $HADOOP_STREAMING_HOME/$STREAMING_JAR \
        -D mapred.fairscheduler.pool="${hdp_pool_name}" \
        -D mapred.job.name="[ads_yew][${hdp_name}]" \
        -D mapred.reduce.tasks="$hdp_reduce_task" \
        -cacheArchive "${PYTHON_LIB}/python27.tar.gz#python27" \
        -input "${hdp_input}" \
        -output "${hdp_output}" \
        -file "${mapper_file}" \
        -file "${reducer_file}" \
        -mapper "${hdp_mapper_cmd}" \
        -reducer "${hdp_reducer_cmd}" \
        ${hdp_adds}
        
       fi
}

function hfsgets2one()
{
    local PID=$$
    local src_prefix=${1}; shift
    local des=${1}; shift

    YWILOG "prepare to download [${src_prefix}*] to [$des]"
    if [[ -f $des && "$MODE" != "RETRY" ]]; then
      YWWLOG "[$des] exists, and NOT in RETRY mode, skip this step"
    else
      if [ -f $des ]; then
        YWWLOG "[$des] exists, in RETRY mode, remove it"
        rm ${des}
      fi

      ` touch $des `

      local rdir=`dirname $src_prefix`
      local rname=`basename $src_prefix`
      local filelist=` hadoop fs -ls $rdir 2>/dev/null | awk '{print $8}' ` 

      for file in $filelist 
      do
          local fcheck=` basename $file | grep "^$rname" `
          if [[ $fcheck != "" ]]; then
              if [[ ${file##*.} == "gz" || ${file##*.} == "bz2" ]]; then
                local tmpfile="/tmp/hfsgets2one_${PID}.${file##*.}"
              else
                local tmpfile="/tmp/hfsgets2one_${PID}.tmp"
              fi
              local tmpfile_wosuffix="/tmp/hfsgets2one_${PID}"
              YWILOG "downloading... $file to $tmpfile, append to $des"
              echo "From: $file"
              echo "To: $tmpfile"
              if [[ -f $tmpfile ]]; then
                  ` rm -f $tmpfile `
              fi
              ` hadoop fs -get $file $tmpfile 2>/dev/null `
              if [[ $? -ne 0 ]]; then 
                  YWWLOG "[ERROR] download from [$file] to [$tmpfile]"
                  exit 1
              fi
              if [[ ${file##*.} == "gz" ]]; then
                gzip -d $tmpfile >> $des
              else
                if [[ ${file##*.} == "bz2" ]]; then
                  bzip2 -d $tmpfile >> $des
                else
                  mv $tmpfile $tmpfile_wosuffix
                fi
              fi
              ` cat $tmpfile_wosuffix >> $des `
              if [[ $? -ne 0 ]]; then 
                  exit 1
              fi
              ` rm -rf $tmpfile_wosuffix `
          fi
      done

    fi
}

function hfsget2local()
{
    local hdp_output=${1}; shift
    local local_res=${1}; shift

    YWILOG "prepare to download [$hdp_output/part*] to [$local_res]"
    if [[ -f $local_res && "$MODE" != "RETRY" ]]; then
        YWWLOG "[$local_res] exists, and NOT in RETRY mode, skip this step"
    else
        if [ -f ${local_res} ]; then
            YWWLOG "[$local_res] exists, in RETRY mode, remove it"
            rm ${local_res}
        fi
        hfsgets2one ${hdp_output}/part ${local_res}
    fi
}

#@brief: test dir on hadoop
#@input: hadoop dir name
#@return: 0:exist 1:not exist
function hfstestdir()
{
    if [ $# -ne 1 ]; then
        echo "USAGE: hfstestdir() hadoop_dirname"
        exit 1;
    fi

    local hdp_dir=${1}

    hadoop fs -ls ${hdp_dir} > /dev/null

    echo $?
}

#@brief: test file on hadoop
#@input: hadoop file name
#@return: 0:exist 1:not exist
function hfstestfile()
{
    if [ $# -ne 1 ]; then
        echo "USAGE: hfstestfile() hadoop_dirname"
        exit 1;
    fi

    local hdp_file=${1}
    
    hadoop fs -ls ${hdp_file} > /dev/null 
    
    echo $? 
}

#@brief: count files on hadoop
#@input: hadoop dir or files path
#@return: file numbers
function hfscountfiles()
{
    if [ $# -ne 1 ]; then
      echo "USAGE: hfscountfiles() hadoop_dirname|filepath"
      exit 1
    fi

    local hdp_path=${1}
    
    num=`hadoop fs -ls ${hdp_path} | wc -l`

    echo ${num}
}


function wget2local() {
    local remote_path=${1}; shift
    local local_path=${1}; shift

    YWILOG "prepare to download [$remote_path] to [$local_path]"
    if [[ -f $local_path && "$MODE" != "RETRY" ]]; then
        YWWLOG "[$local_path] exists, and NOT in RETRY mode, skip this step"
    else
        if [ -f ${local_path} ]; then
            YWWLOG "[$local_path] exists, in RETRY mode, remove it"
            rm ${local_path}
        fi

        wget $remote_path -O ${local_path}

        if [ "$?" != 0 ]; then
            YWFLOG "download [$remote_path] to [$local_path] failed, will remove [$local_path]"
            rm ${local_path}
        fi
    fi
}

function scp2local() {
    local remote_path=${1}; shift
    local loacl_path=${1}; shift

    YWILOG "prepare to download [$remote_path] to [$local_path]"
    if [[ -f $local_path && "$MODE" != "RETRY" ]]; then
        YWWLOG "[$local_path] exists, and NOT in RETRY mode, skip this step"
    else
        if [ -f ${local_path} ]; then
            YWWLOG "[$local_path] exists, in RETRY mode, remove it"
            rm ${local_path}
        fi

        scp $remote_path ${local_path}

        if [ "$?" != 0 ]; then
            YWFLOG "download [$remote_path] to [$local_path] failed, will remove [$local_path]"
            rm ${local_path}
        fi
    fi
}

function getpwd() {
    cur_dir=`pwd`
    s_dir=`dirname ${1}`
    if [ "${s_dir}" == "." ]; then
        script_dir="$cur_dir"
    elif [ "${s_dir:0:2}" == "./" ]; then
        script_dir="$cur_dir/${s_dir:2:1024}"
    else
        script_dir="${s_dir}"
    fi  
    local_dir=`dirname $script_dir`
    bin_dir="$local_dir/bin"
    res_dir="$local_dir/res"
}

#报警电话 曾杰瑜 梁伟
#在外围配置文件feature_extract.conf配置
#TELPHONES=(18600574510 18611453223)
#TELPHONES=(15801436855)
function send_alarm()
{
  MESSAGE="[`hostname`]"$1
  echo $TELPHONES
  YWILOG "Sending alarm :【$MESSAGE】"
  
  wget -q -O /dev/null "http://sms.notify.d.xiaonei.com:2000/receiver?number=${TELPHONES}&message=$MESSAGE";
  
  #for tel in ${TELPHONES[@]}; do
  #  wget -q -O /dev/null "http://sms.notify.d.xiaonei.com:2000/receiver?number=${tel}&message=$MESSAGE";
  #done

}

function check_hdp_data()
{
  FILE_PATH=$1
  
  YWILOG "Check data of ${FILE_PATH}" 

  #文件不存在报警
  #hadoop1.03版本-dus结果顺序与0.21相反，所以print $2
  file_size=`hadoop fs -dus ${FILE_PATH} | awk '{print $2}'`
  
  YWILOG "file_size: $file_size"
  YWILOG "[ $file_size -le 0 ]"

  if [ "X${file_size}" = "X" ]; then
    MESSAGE="$FILE_PATH not exists." 
    send_alarm "$MESSAGE"
    return 
  elif [ ${file_size} -le 0 ]; then
    MESSAGE="$FILE_PATH is empty." 
    send_alarm "$MESSAGE"
    return
  fi
  
  YWILOG "Data of ${FILE_PATH} OK" 
}
#xc_day.sh 报警
function check_hdp_daily_data()
{
  HDP_ROOT_DIR=$1; shift
  CHECK_DATE=$1; shift  
  STAGE=$1; shift
  
  #local HDP_ROOT_DIR="/user/wen.ye/m1fe"
  if [[ $STAGE = "r0" ]]; then
    DAILY_DATA_PATH="${HDP_ROOT_DIR}/daily_validpv/${CHECK_DATE}" 
  elif [[ $STAGE = "r1" ]]; then
    DAILY_DATA_PATH="${HDP_ROOT_DIR}/daily/${CHECK_DATE}" 
  elif [[ $STAGE = "r5" ]]; then
    DAILY_DATA_PATH="${HDP_ROOT_DIR}/xdaily/${CHECK_DATE}" 
  elif [[ $STAGE = "r4" ]]; then
    DAILY_DATA_PATH="${HDP_ROOT_DIR}/newdaily/${CHECK_DATE}" 
  else
    return
  fi 
  
  check_hdp_data "$DAILY_DATA_PATH"
}
#xc_hour.sh报警
function check_hdp_hourly_data()
{
  HDP_ROOT_DIR=$1; shift
  PROCESS_HOUR="${1}"; shift
  #处理阶段
  STAGE=$1; shift
   
  if [ $STAGE = "r0" ]; then
    HOUR_DATA_PATH="$HDP_ROOT_DIR/hourly_validpv/$PROCESS_HOUR"
  elif [ $STAGE = "r1" ]; then
    HOUR_DATA_PATH="$HDP_ROOT_DIR/hourly/$PROCESS_HOUR"
  elif [ $STAGE = "r5" ]; then
    HOUR_DATA_PATH="$HDP_ROOT_DIR/xhourly/$PROCESS_HOUR"
  else
    return
  fi

  check_hdp_data "$HOUR_DATA_PATH"

}
#yc_genm.sh 生成本地文件报警
function check_local_file()
{
  FILE_PATH=$1
  
  if [ -e $FILE_PATH ] ; then
    if [ ! -s $FILE_PATH ]; then
      MESSAGE="$FILE_PATH is empty"
      send_alarm  "$MESSAGE" 
      return
    fi
  else
    MESSAGE="$FILE_PATH is not exist."
    send_alarm  "$MESSAGE"
    return
  fi  
   
  YWILOG "Data of $FILE_PATH OK"
  

}

#@brief: lock hdfs上的文件,lock与unlock必须配对使用
#@intpu: base_dir: lock文件写入hdfs的目录
#        file: 需要锁定的文件，为hdfs路径，文件或者目录
#        pid:  锁定该文件的进程id
#        flag: 锁类型,R/W 读锁写锁，写独占，包括删除，读共享
#        timeout: 获得锁等待的最长时间,单位秒(s),时间不精确
#@return: 0:success 1:file_not_exist 2:timeout 9:para_error
function lock()
{
  if [ $# -ne 5 ]; then
    YWWLOG "Parameter error."
    YWWLOG "Usage: lock base_dir file pid flag[R/W] timeout."
    return 9
  fi

  base_dir=$1
  file=$2
  pid=$3
  flag=$4
  timeout=$5

  if [[ "X$flag" != "XR" ]] && [[ "X$flag" != "XW" ]]; then
    YWWLOG "[ERROR] flag must be W or R"
    return 9
  fi  

  #生成的lock标记以pid加"_"连接文件的形式
  #以"/"分割的第一个元素为空，所以i=2
  #lock与unlock必须配对使用
  fileName=`echo $file | awk -F "/" '{i=2; filename =""; while(i<=NF){ filename = filename"_"$i; i = i+1;} print filename}'`
  lockFileName="${pid}${fileName}_${flag}"  #本进程lock的文件名
  if [ "X${flag}" = "XW" ]; then        #匹配其它进程lock的文件名，写锁匹配任意锁
    testFileName="*${fileName}_*"
  else
    testFileName="*${fileName}_W"       #匹配其它进程lock的文件名，读锁匹配写锁就OK
  fi
  time=1
  count=`echo "${timeout}/10 + 1" | bc` #每10s检测一次，减少检测次数
  while [ $time -le $count ]
  do
    hadoop fs -test -e $file
    if [ $? -ne 0 ]; then
      YWWLOG "[ERROR] $file not exist."
      return 1
    fi
    hadoop fs -ls ${base_dir}/${testFileName} &> /dev/null
    if [ $? -ne 0 ]; then
      hadoop fs -mkdir ${base_dir}/${lockFileName} &> /dev/null
      YWILOG "$file locked"
      return 0
    fi

    ((time = time + 1))
    YWILOG "$file be locked by other program, waiting for a while ..."
    sleep 10s
  done
  
  YWWLOG "[ERROR] can not lock $file within time:$timeout"
  return 2
}

#@brief: unlock hdfs上的文件,lock与unlock必须配对使用
#@intpu: base_dir: lock文件所在的hdfs目录
#        file: 需要解锁的文件，为hdfs路径或文件
#        pid: 锁定该文件的进程id
#@return: 0:success 9:para_error
function unlock()
{
  if [ $# -ne 3 ]; then
    YWWLOG "Parameter error."
    YWWLOG "Usage: unlock base_dir file pid."
    return 9
  fi
    
  base_dir=$1
  file=$2
  pid=$3

  #文件锁的名称,lock与unlock必须配对使用
  lockFileName=`echo $file | awk -F "/" -v pid=${pid} '{i=2; filename = pid; while(i<=NF){ filename = filename"_"$i; i = i+1;} print filename}'`
  hadoop fs -rmr ${base_dir}/${lockFileName}* &> /dev/null  #利用pid即可，加*匹配文件名
  YWILOG "$file unlocked"
  return 0
  
}

#@brief: check locks
#        检查hdfs根路径下的所有文件是否被锁住时间太长
#        如果超过指定时间锁将被取消
#@input: base_dir: lock文件所在的hdfs目录
#        lock_file_path: 需要检查的文件根路径，hdfs路径
#        timeout: 被锁住的超时时间, optional, 单位s
#@return: 0:unlocked 1:still_locked 9:para_error
function check_locks()
{
  if [ $# -ne 2 -a $# -ne 3 ]; then
    YWWLOG "Parameter error."
    YWWLOG "Usage: check_locks base_dir lock_file_root_path locked_timeout[optional]."
  fi
  
  base_dir=$1
  root_file=$2
  timeout=0
  if [ $# -eq 3 ]; then
    timeout=$3
  fi

  lockedFileName=`echo ${root_file} | awk -F "/" '{i=2; filename = ""; while(i<=NF){ filename = filename"_"$i; i = i+1;} print filename}'`
  now=`date +%Y-%m-%d" "%H:%M:%S`
  now=`date -d "$now" +%s`
  
  YWILOG "check_lock for FileName: $lockedFileName"
  hadoop fs -ls ${base_dir} | grep ${lockedFileName} > lock_temp
  
  while read item
  do
    nf=`echo ${item} | awk '{print NF}'`
    if [ $nf -ne 8 ]; then
      continue
    fi
    start_lock_time=`echo ${item} | awk '{print $6" "$7}' `
    start_lock_time=`date -d "$start_lock_time" +%s`

    file=`echo ${item} | awk '{print $8}' `
    time_gap=`echo "$now - $start_lock_time" | bc`
    YWILOG "time_gap: ${time_gap}s    timeout: ${timeout}s locked_file[$file]"
    if [ $time_gap -ge $timeout ]; then
      YWILOG "hadoop fs -rmr $file"
      send_alarm "[NOTICE] [$file] locked langer than $timeout, now unlocked." 
      hadoop fs -rmr $file
    fi
  done < lock_temp
  rm -rf lock_temp

}

#@brief: gen using dates
#        根据起始日期和结束日期生成日期序列(左闭右开)
#@input: START_DATE: 开始日期
#        END_DATE: 结束日期
#@return: 日期序列，格式: {date1,date2,date...}
function get_process_dates() {
  if [[ $# -ne 2 ]]; then
    echo "paramater error"
    exit 1
  fi

  local START_DATE=${1}; shift
  local END_DATE=${1}; shift
  #echo $START_DATE  $END_DATE
  local TMP_DATE=$START_DATE
  local PROCESSING_DATES="{$START_DATE"
  while [[  $TMP_DATE -ne $END_DATE ]]
  do
    #echo "$TMP_DATE  $END_DATE"
    TMP_DATE=`date -d "1 days $TMP_DATE" +%Y%m%d`
    PROCESSING_DATES="${PROCESSING_DATES},$TMP_DATE"
  done
  PROCESSING_DATES="$PROCESSING_DATES}"

  echo ${PROCESSING_DATES}
}

#@brief: gen using dates and hours
#        根据起始日期小时和结束日期小时生成日期小时序列(左闭右开)
#@input: START_DATE: 开始日期小时
#        END_DATE: 结束日期小时
#@return: 日期序列，格式: {datehour1,datehour2,datehour...}
function get_process_dateHours() {
  if [[ $# -ne 2 ]]; then
    echo "paramater error"
    exit 1
  fi

  local START_DATE=${1}; shift
  local END_DATE=${1}; shift
  #echo $START_DATE  $END_DATE
  local TMP_DATE=$START_DATE
  local PROCESSING_DATES="{$START_DATE"
  while [[  $TMP_DATE -ne $END_DATE ]]
  do
    #echo "$TMP_DATE  $END_DATE"
    TMP_DATE=`date -d "1 hours ${TMP_DATE:0:8} ${TMP_DATE:8:2}" +%Y%m%d%H`
    PROCESSING_DATES="${PROCESSING_DATES},$TMP_DATE"
  done
  PROCESSING_DATES="$PROCESSING_DATES}"

  echo ${PROCESSING_DATES}
}
