#!/bin/bash
 
op_name=$1
echo "OPERATOR ---------- $op_name"
VAR=""
argv=""

module_name=etl_module
main_module=etl_edr
process_name=etl_edr_p1_p2
p3_process=etl_edr_p3
log_level="INFO" # INFO, DEBUG, ERROR

source $commonConfigurationFilePath  > /dev/null
eval "argv=\${${op_name}_source_edr}"
echo "Sources Name --" ${argv}

all_folder="all_edr"
output_folder="output"

# If changing p1 input dir ie cdr_input , this need to be update 
p2_input_path="${DATA_HOME}/cdr_input/$op_name/$all_folder/"

app_path="${APP_HOME}/${module_name}/${main_module}/${process_name}/"

base_path_output="${DATA_HOME}/${module_name}/${main_module}/${process_name}_output/${op_name}"

p2_output_path="${DATA_HOME}/$module_name/$main_module/${process_name}_output/$op_name/$all_folder/output/"

p3_input_path="${DATA_HOME}/$module_name/$main_module/${p3_process}_input/$op_name/"

build=${process_name}.jar

start_java_process(){
log_path="${LOG_HOME}/${module_name}/${main_module}/$process_name/$1/$2/"

status=`ps -ef | grep $main_module | grep $build $1 $2 | grep -v grep | grep -v vi | grep java`

if [ "${status}" != "" ]  ## Process is currently running
then
  echo "${module_name} $process_name $1 $2  already started..."
else  ## No process running
  echo "Starting $process_name $1 $2 Process..."

  mkdir -p ${log_path}
 
  java -Dlog_level=${log_level} -Dlog_path=${log_path} -Dmodule_name=${process_name}_${2}  -Dlog4j.configurationFile=./log4j2.xml -Dspring.config.location=file:./application.properties,file:${commonConfigurationFilePath} -jar ${build} $1 $2 1>/dev/null 2>${log_path}/${process_name}_${2}.error  &

fi

}

cd $app_path/
echo "P1_P2 start at $(date "+%Y-%m-%d-%H:%M:%S")"

count=0
i=0
for j in ${argv//,/ }
do
	array[$i]=$j;
	echo "for ${array[$i]}"
	start_java_process "$op_name" "${array[$i]}" &
	count=$((count+1))
	sleep 5
done

echo "waiting for instances to end"
status_final=`ps -ef | grep $main_module |  grep $build |grep -v vi |  grep $1 | wc -l`

while [ "$status_final" -gt 0 ]
do
   echo "instances running i.e.- `ps -ef | grep $build |grep -v vi |  grep $1`"
   status_final=`ps -ef | grep $main_module |  grep $build |grep -v vi |  grep $1 | wc -l`
   sleep 15
done

wait $!

echo "Milestone-P1 End  $(date "+%Y-%m-%d-%H:%M:%S")"	

echo "P1 files Move start at$(date "+%Y-%m-%d-%H:%M:%S")"	

for j in ${argv//,/ }
do
	array[$i]=$j;
	echo "Move From=" "$base_path_output"/${array[$i]}/"$output_folder" " To=" "$p2_input_path"
	mv "$base_path_output"/${array[$i]}/"$output_folder"/* "$p2_input_path"  > /dev/null
	if [ $? != 0 ]
	then
		echo "No File/Folder present for ${array[$i]}"
	else
		echo "output file for ${array[$i]} sucessfully moved to all folder"
	fi
done

echo "Milestone-P1 File Moving End $(date "+%Y-%m-%d-%H:%M:%S")"
 
start_java_process "$op_name" "$all_folder"

t1=$?
sleep 5
if [ "$t1" != 0 ]
then
	echo "P2 failed to start"
	exit 1
else
	echo "P2 started"
fi
 
status_P2=`ps -ef | grep $main_module |  grep $build |grep -v vi |  grep $all_folder | wc -l`

while [ "$status_P2" -gt 0 ]
do
   echo "instances running i.e.- `ps -ef | grep $build |grep -v vi |  grep $all_folder`"
   status_P2=`ps -ef | grep $main_module |  grep $build |grep -v vi |  grep $all_folder | wc -l`
   sleep 15
done

wait $!

echo "Milestone-P2 End  $(date "+%Y-%m-%d-%H:%M:%S")"	

echo "P2 splited Files movement Start $(date "+%Y-%m-%d-%H:%M:%S")"

count=11
cd $p2_output_path

for filename in `ls -tr $p2_output_path` ; do

if [ $j -lt $count ]
	then
		if [ -r "$filename" ]
		then
                        echo $filename
			mv $filename $p3_input_path/$j/process/
			j=$((j+1))
		else
			continue
		fi
	else
		j=1
		mv $filename $p3_input_path/$j/process/
	        j=$((j+1))
	fi
done

echo "Milestone- P2 splited Files moved End $(date "+%Y-%m-%d-%H:%M:%S")"	

echo "Starting P3"

cd ${APP_HOME}/$module_name/$main_module/$p3_process/

./allOpertorEdr.sh $op_name &

echo "Run Process END $(date "+%Y-%m-%d-%H:%M:%S")"

exit 0
