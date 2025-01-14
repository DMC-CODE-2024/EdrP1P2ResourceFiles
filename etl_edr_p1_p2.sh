#!/bin/bash

#set -x

module_name="etl_edr_p1_p2"
main_module="etl_edr" #keep it empty "" if there is no main module 
log_level="INFO" # INFO, DEBUG, ERROR

########### DO NOT CHANGE ANY CODE OR TEXT AFTER THIS LINE #########
op_name=$1

build_path="${APP_HOME}/${main_module}_module/${module_name}"
build="${module_name}.jar"

cd ${build_path}

source application.properties > /dev/null
start_java_process()
{
  op_name=$1 
  source_name=$2

  log_path="${LOG_HOME}/${main_module}_module/${module_name}/${op_name}/${source_name}"
  
  mkdir $log_path -p

  status=`ps -ef | grep $build | grep java | grep $module_name | grep ${op_name} | grep ${source_name} | grep -v grep`
  if [ "${status}" != "" ]  ## Process is currently running
  then
    echo "$(date) ${module_name} [${op_name}]-[${source_name}]: ${module_name} $1 $2 already started..."

  else  ## No process running

    echo "$(date) ${module_name} [${op_name}]-[${source_name}]: starting java process for ${module_name} $op_name $source_name..."

    file_count=`ls -1 $INPUTPATH/${op_name}/${source_name} | wc -l`

    echo "$(date) ${module_name} [${op_name}]-[${source_name}]: total file in $source_name input folder = ${file_count} ..."

    cd ${build_path}

#    java -Dlog.level=${log_level} -Dlog.path=${log_path} -Dmodule.name=${module_name}_${2}  -Dlog4j.configurationFile=./log4j2.xml -Dspring.config.location=file:./application.properties,file:${commonConfigurationFilePath} -jar ${build} ${op_name} ${source_name} 1>/dev/null 2>${log_path}/${module_name}_${2}.error  &

    java -Dlog.level=${log_level} -Dlog.path=${log_path} -Dmodule.name=${module_name}_${2}  -Dlog4j.configurationFile=./log4j2.xml -Dspring.config.location=file:./application.properties,file:${commonConfigurationFile} -jar ${build} ${op_name} ${source_name} 1>/dev/null 2>${log_path}/${module_name}_${2}.error  &

    echo "$(date) ${module_name} [${op_name}]-[${source_name}]: java process for ${module_name} $op_name $source_name started..."

  fi

}

##------------------##
## Start P1 process ##
##------------------##

eval "source_list=\${${op_name}_source}"  ## get source list from config file

echo "$(date) ${module_name} [${op_name}]: ==> starting P1 process ..."

echo "$(date) ${module_name} [${op_name}]: data source list : ${source_list}"


for j in ${source_list//,/ }
do
  start_java_process "${op_name}" "${j}" &
  sleep 3
done


echo "$(date) ${module_name} [${op_name}]: waiting P1 java process for all ${op_name} data sources to complete..."

status_final=`ps -ef | grep $build | grep java | grep $module_name | grep ${op_name} | grep -v grep | wc -l`

while [ "$status_final" -gt 0 ]
do
  sleep 15
  status_final=`ps -ef | grep $build | grep java | grep $module_name | grep ${op_name} | grep -v grep | wc -l`
done

wait $!

echo "$(date) ${module_name} [${op_name}]: P1 java process is completed for all ${op_name} data sources..."

## moving file from operator source wise to operator all folder 

p1_output_path=${OUTPUTPATH}/${op_name}
p2_input_path=${INPUTPATH}/${op_name}/${all_folder}


for j in ${source_list//,/ }
do
  source_name=$j

  mkdir $p1_output_path/${source_name}/ -p

  f_count=`ls -tr ${p1_output_path}/${source_name} | wc -l`

  if [ ${f_count} == 0 ]
  then

    echo "$(date) ${module_name} [${op_name}]-[${source_name}]: no P1 output file for ${source_name}..."

  else

    mkdir $p2_input_path -p

    echo "$(date) ${module_name} [${op_name}]-[${source_name}]: start moving P1 output file = ${f_count} from ${p1_output_path}/${source_name}/* to $p2_input_path"

    mv ${p1_output_path}/${source_name}/* ${p2_input_path}

    echo "$(date) ${module_name} [${op_name}]-[${source_name}]: P1 output file = ${f_count} sucessfully moved to ${p2_input_path}"

  fi

done

echo "$(date) ${module_name} [${op_name}]: ==> P1 process is completed !!! "

##------------------##
## Start P2 Process ##
##------------------##

echo "$(date) ${module_name} [${op_name}]: ==> starting P2 process ..."

start_java_process "$op_name" "$all_folder"

echo "$(date) ${module_name} [${op_name}]: waiting P2 java process for${op_name} to complete..."

status=`ps -ef | grep $build | grep java | grep $module_name | grep ${op_name} | grep ${all_folder} | grep -v grep | wc -l`

while [ "$status_final" -gt 0 ]
do
  sleep 15
  status=`ps -ef | grep $build | grep java | grep $module_name | grep ${op_name} | grep ${all_folder} | grep -v grep | wc -l`
done

wait $!

echo "$(date) ${module_name} [${op_name}]-[${all_folder}]: P2 java process is completed..."

## moving P2 splited file to P3 input path 

p2_output_path="${OUTPUTPATH}/${op_name}/${all_folder}/"
p3_input_path="${DATA_HOME}/${main_module}_module/${main_module}_p3/input/${op_name}"

mkdir $p2_output_path -p

f_count=`ls -tr $p2_output_path | wc -l`

if [ ${f_count} == 0 ]
then 
  echo "$(date) ${module_name} [${op_name}]-[${source_name}]: no P2 output file for ${source_name}..."

else

  echo "$(date) ${module_name} [${op_name}]-[${all_folder}]: start moving P2 output splited file from ${p2_output_path} to ${p3_input_path}"

  count=10  ## define number of splited files
  j=1

  cd $p2_output_path

  for filename in `ls -tr $p2_output_path` ;
  do
    if [ -r "$filename" ]  ## Check if file is good 
    then

      mkdir $p3_input_path/$j -p

      if [ $j -le $count ]
      then
        echo "$(date) ${module_name} [${op_name}]-[${all_folder}]: start moving P2 output splited file ${filename} to ${p3_input_path}/$j"
	mv $filename $p3_input_path/$j
        j=$((j+1))

      else
        j=1
        echo "$(date) ${module_name} [${op_name}]-[${all_folder}]: start moving P2 output splited file ${filename} to ${p3_input_path}/$j"
        mv $filename $p3_input_path/$j
        j=$((j+1))
      fi
    fi
  done

  echo "$(date) ${module_name} [${op_name}]-[${all_folder}]: P2 output splited files are sucessfully moved to ${p3_input_path}/..."

fi

echo "$(date) ${module_name} [${op_name}]: ==> P2 process is completed !!! "

##----------------------##
## Call next p3 process ##
##----------------------##

echo "$(date) ${module_name} [${op_name}]: ==> calling next P3 process... "

cd "${APP_HOME}/${main_module}_module/${main_module}_p3"

./${main_module}_p3.sh ${op_name}  
