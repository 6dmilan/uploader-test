#!/bin/bash
path=$0
scriptName=`basename $path`
length=`expr ${#path} - ${#scriptName}`
substr=${path:0:$length}
#loading the properties files
if [ $# -ne 1 ]
	then
	echo "----SCRIPT EXISTING !!! USAGE IS :: ${substr}/${scriptName} APP_NAME ----" 
	exit 1
fi
appname=$1
if [ "$substr" = "" ] || [ "$substr" = "./" ]
	then
	substr="./"
	. common.properties
	. ${appname}.properties
else
	. ${substr}common.properties
	. ${substr}${appname}.properties
fi

echo "----- Properties files path :: \" ${substr} \"  :: files are :: \" ${substr}common.properties \" :: \" ${substr}${appname}.properties \" -----"

#Entering Uploading Script
#making the path for logs (If it doesn't exist already)
mkdir -p ${script_logs_path}
#Additional Logger
#timeStamp=`(date --date='1 hours ago' '+%H%M%S')`
#table_prefix_key="${table_prefix_key}${timeStamp}"
#=================================================
#touch command will create the file if it is not there.otherwise it will update the last modified time to current time
touch ${script_logs_path}/${table_prefix_key}.log
mv ${script_logs_path}/${table_prefix_key}.log ${script_logs_path}/${table_prefix_key}.logging

log_file=${script_logs_path}/${table_prefix_key}.logging

#setting the delimiter.if it is over-ridden in the individual properties file making use of that only otherwise taking from the common.properties file
if [ "$delimiter" = "" ]
then
  delimiter=$common_delimiter
fi

#this is required while moving the files to backup directory
today_date=`date '+%d_%m_%Y'`
ystrday_date=`date --date='1 days ago' '+%d_%m_%Y'`

prefix_length=`expr $file_prefix : '.*'`


if [ -d ${daily_logs} ];
  then
    logdetail=${daily_logs}/${daily_log_file_prefix}_`date +"%d_%m_%Y"`.log
    errorstatus=${daily_logs}/errorstatus.txt
    rmLogName=`(date --date="${daily_logs_backup_days} days ago" "+${daily_log_file_prefix}_%d_%m_%Y.log")`
    rm -f ${daily_logs}/$rmLogName
 else
    mkdir  ${daily_logs}
    logdetail=${daily_logs}/${daily_log_file_prefix}_`date +"%d_%m_%Y"`.log
    errorstatus=${daily_logs}/errorstatus.txt
fi

NoofAttempts=1
#Getting the details
fun_GetParams(){
mysql -h$cdr_db_ip -P$cdr_db_port  -u$cdr_db_user -p$cdr_db_pwd $cdr_db_name -sse "select getParamValue('$table_prefix_key')" 2>&1 | tee $errorstatus > ${appname}_table_prefix.txt
mysql -h$cdr_db_ip -P$cdr_db_port  -u$cdr_db_user -p$cdr_db_pwd $cdr_db_name -sse "select getParamValue('$columns_key')" 2>&1 | tee $errorstatus > ${appname}_columns.txt
mysql -h$cdr_db_ip -P$cdr_db_port  -u$cdr_db_user -p$cdr_db_pwd $cdr_db_name -sse "select getParamValue('$app_start_date_key')" 2>&1 | tee $errorstatus  > ${appname}_app_start_date.txt

errorcode=`grep ERROR $errorstatus | awk -F' ' {'print $2'}`
if [ "$errorcode" != "" ];then
    echo $(grep ERROR $errorstatus) >> $logdetail
	    echo "error occured  while  calling getParams is ERROR $errorcode"  >> $logdetail
		  if [ $errorcode -eq 1040 ]; then
		        echo "error occured due to toomany connections calls check conn method"  >> $logdetail
				 checkconngetParams
		  elif [ $errorcode -gt 1000 -a $errorcode -lt 3000 ]; then
			   echo "trying to  get the configparams  again" >> $logdetail
		       echo "no of retry attemts is $((NoofAttempts++))" >> $logdetail

	if [ $NoofAttempts = 11 ];then
	      NoofAttempts=1
		  exit 1
		else
     	 sleep 30
	     fun_GetParams
   fi
 fi
fi
}
checkconngetParams()
{
CONNS=`netstat -antp | grep $cdr_db_port | wc -l`
if [ $CONNS -lt ${maxConns} ]
then
      echo "connections is $CONNS ,so calling getParams again"  >> $logdetail
      fun_GetParams
 else 
	   echo "no of current connections is $CONNS"  >> $logdetail
	   sleep 30
	   checkconngetParams
 fi
}
#calling GetParams
fun_GetParams

table_prefix=`tail -1 ${appname}_table_prefix.txt`
columns=`tail -1 ${appname}_columns.txt`
start_day_secs=$( date -d `tail -1 ${appname}_app_start_date.txt` +%s )

rm -rf ${appname}_table_prefix.txt ${appname}_columns.txt ${appname}_app_start_date.txt 

echo "Uploading Process Started at `date '+%d/%m/%Y %T'`" 1>> $log_file 2>> $log_file
echo "................................................" 1>> $log_file 2>> $log_file
echo "...........Uploading Process Started at `date '+%d/%m/%Y %T'`.............." >> $logdetail

echo "Executing with the following parameters" 1>> $log_file 2>> $log_file
echo "${appname}_table_prefix="$table_prefix 1>> $log_file 2>> $log_file
echo "${appname}_columns="$columns 1>> $log_file 2>> $log_file
echo "${appname}_cdrfile_path="$cdrfile_path 1>> $log_file 2>> $log_file
echo "${appname}_output_path="$output_path 1>> $log_file 2>> $log_file
echo "${appname}_file_prefix="$file_prefix 1>> $log_file 2>> $log_file
echo "${appname}_file_postfix="$file_postfix 1>> $log_file 2>> $log_file
echo "start_day_secs="${start_day_secs} 1>> $log_file 2>> $log_file
echo "" 1>> $log_file 2>> $log_file
echo  "columns of Loading  CDR table == "$columns" " >> $logdetail

i=0
for filename in `find $cdrfile_path -name "$file_prefix*$file_postfix"`
do
  i=`expr $i + 1`
  
  echo "Processing of ${filename} is Started at `date '+%d/%m/%Y %T'`" 1>> $log_file 2>> $log_file
 
  file=`basename ${filename}`

  date=${file:$prefix_length:14}

  year=${date:0:4}
  month=${date:4:2}
  day=${date:6:2}
  hour=${date:8:2}

  curr_file_date=${month}/${day}/${year}
  curr_file_secs=$( date -d $curr_file_date +%s )
  table_index=`expr $( expr $( expr $curr_file_secs - $start_day_secs )  / 86400 ) % 2` #86400 secs per day
 
  if [ "$table_index" = "1" ]
  then
  	 table_index=01
  elif [ "$table_index" = "0" ]
  then
    table_index=02
  fi
  
  table_name=${table_prefix}${month}

  echo "table_name & curr_file_secs="$table_name" & "$curr_file_secs 1>> $log_file 2>> $log_file
wait

funCdrLoad(){ 
   echo "Loading the   ${filename} to CDR TABLE "$table_name"  started  at `date '+%d/%m/%Y %T'`"  >> $logdetail
   echo "Loading the data from file ${filename} into CDR Table at `date '+%d/%m/%Y %T'`" 1>> $log_file 2>> $log_file

   if [ "${isCDRSecondaryReplicationEnabled}" = "TRUE" ]
   then
	   echo "load data CONCURRENT local infile '$filename' into table $table_name fields terminated by '$delimiter' lines  terminated by '\n'  ($columns) SET FILE_NAME='${filename}'" 1>> $log_file 2>> $log_file
mysql -h$cdr_db_ip -P$cdr_db_port  -u$cdr_db_user -p$cdr_db_pwd $cdr_db_name <<- EOF 2>&1 | tee -a $log_file 1>$errorstatus
		set sql_log_bin=0;
		set autocommit=0;
		load data CONCURRENT local infile '$filename' into table $table_name fields terminated by '$delimiter' lines  terminated by '\n'  ($columns) SET FILE_NAME='${filename}';
		commit;
EOF
		wait

	   echo "Loading the data from file ${filename} into Secondary CDR Table at `date '+%d/%m/%Y %T'`" 1>> $log_file 2>> $log_file
mysql -h$cdr_db_ip_secondary -u$cdr_db_user_secondary -p$cdr_db_pwd_secondary -P$cdr_db_port_secondary $cdr_db_name_secondary <<- EOF 2>&1 | tee -a $log_file 1>$errorstatus
	   set sql_log_bin=0;
	   set autocommit=0;
	   load data CONCURRENT local infile '$filename' into table $table_name fields terminated by '$delimiter' lines  terminated by '\n'  ($columns) SET FILE_NAME='${filename}';
	   commit;
EOF
   else
	   echo "load data CONCURRENT local infile '$filename' into table $table_name fields terminated by '$delimiter' lines  terminated by '\n'  ($columns) SET FILE_NAME='${filename}'" 1>> $log_file 2>> $log_file

mysql -h$cdr_db_ip -P$cdr_db_port  -u$cdr_db_user -p$cdr_db_pwd $cdr_db_name <<- EOF 2>&1 | tee -a $log_file 1>$errorstatus
	   set autocommit=0;
	   load data CONCURRENT local infile '$filename' into table $table_name fields terminated by '$delimiter' lines  terminated by '\n'  ($columns) SET FILE_NAME='${filename}';
	   commit;
EOF
   fi



errorcode=`grep ERROR $errorstatus | awk -F' ' {'print $2'}`
if [ "$errorcode" != "" ];then
    echo $(grep ERROR $errorstatus) >> $logdetail
    echo "error occured  while  loading file ${filename} to CDR  is ERROR  $errorcode" >> $logdetail
  if [ $errorcode -eq 1040 ]; then
      echo "error occured due to toomany connections calls check conn method" >> $logdetail 
      checkconnCdrLoad
  elif [ $errorcode -gt 1000 -a $errorcode -lt 3000 ]; then
        echo "trying to  cdrloaddata again" >> $logdetail
        echo "no of retry attempts is $((NoofAttempts++))" >> $logdetail
       if [ $NoofAttempts = 11 ];then
         NoofAttempts=1
         exit 1
       else
         sleep 30
         funCdrLoad
      fi
  fi
fi

}

checkconnCdrLoad()
{
CONNS=`netstat -antp | grep $cdr_db_port | wc -l`
if [ $CONNS -lt ${maxConns} ]
then
   echo "connections is $CONNS ,so calling cdrloading again" >> $logdetail
   funCdrLoad
else 
     echo "no of current connections is $CONNS" >> $logdetail
     sleep 60
     checkconnCdrLoad
fi
}
#calling getCdrLoad
funCdrLoad

  echo "Loading and processing of  ${filename} completed in  DB `date '+%d/%m/%Y %T'`" >>  $logdetail
  echo "Successfully Loaded the data from file ${filename} into DB `date '+%d/%m/%Y %T'`" 1>> $log_file 2>> $log_file
 
  echo "Processing of ${filename} is Completed at `date '+%d/%m/%Y %T'`" 1>> $log_file 2>> $log_file

wait

  file_date=${day}_${month}_${year}
  file_dir=${output_path}/${file_prefix}${file_date}
  if [ $file_date = $today_date ]
  then
    if [ ! -d "$file_dir" ]
    then
      mkdir $file_dir
    fi
    echo "Moving ${filename} from $cdrfile_path to $file_dir" 1>> $log_file 2>> $log_file
    mv ${filename} $file_dir 2>> $logdetail
  elif [ $file_date = $ystrday_date ]
  then
    if [ -e "${file_dir}.zip" ]
    then
      mkdir $file_dir
      echo "Moving ${filename} from $cdrfile_path to $file_dir" 1>> $log_file 2>> $log_file
      mv ${filename} $file_dir 2>> $logdetail
      cd ${output_path}
      zip -rq ${file_dir}.zip ${file_prefix}${file_date}
      rm -rf $file_dir
      cd -
    else
      mkdir $file_dir
      echo "Moving ${filename} from $cdrfile_path to $file_dir" 1>> $log_file 2>> $log_file
      mv ${filename} $file_dir 2>> $logdetail
    fi
  else
    mkdir $file_dir
    echo "Moving ${filename} from $cdrfile_path to $file_dir" 1>> $log_file 2>> $log_file
    mv ${filename} $file_dir 2>> $logdetail
    cd ${output_path}
    zip -rq ${file_dir}.zip ${file_prefix}${file_date}
    rm -rf $file_dir
    cd -
  fi
 
  echo "" 1>> $log_file 2>> $log_file
  echo "" 1>> $log_file 2>> $log_file
done

if [ -f $errorstatus ];then
rm -f $errorstatus
fi

cd $output_path 1>> $log_file 2>> $log_file
ystrday_dir=${output_path}/${file_prefix}${ystrday_date}
if [ -d "$ystrday_dir" ]
then
  zip -rq ${file_prefix}${ystrday_date}.zip ${file_prefix}${ystrday_date}
  rm -rf ${file_prefix}${ystrday_date}
fi
cd -

echo "Number of Files Processed = $i" 1>> $log_file 2>> $log_file
if [ $i = 0 ]
then
  echo "No files to process at this time" 1>> $log_file 2>> $log_file
fi

echo "" 1>> $log_file 2>> $log_file
echo "" 1>> $log_file 2>> $log_file
echo "" 1>> $log_file 2>> $log_file
echo "" 1>> $log_file 2>> $log_file

#this should be the last statement in the shell script file
mv ${script_logs_path}/${table_prefix_key}.logging ${script_logs_path}/${table_prefix_key}.log
