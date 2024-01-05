#!/bin/bash
currPath="/home/milan/MY_UTILITIES/CDRUpload/CDRUploadingScripts_1.0/APP_CDRUploadScripts" 
thisScriptName=`basename $0`
echo "_ _ _ _ _ _ _ Started CDR Uploading at `date "+%Y-%b-%d %H:%M:%S"` _ _ _ _ _ _ _"
runningFile=${currPath}/.ExecutionStatus.txt
if [ -f ${runningFile} ]
	then
	ps -aef | grep ${thisScriptName} | grep -v grep
	echo -e "\n<Error>${thisScriptName} is Already Running...So Current instance is exiting !! \n\n<Warning> Please check whether the script is actually in execution, if not,unexpected termination of previous execution of the script may have left the file \" ${runningFile}\" unremoved.Remove the file \"${runningFile}\" if the script is still terminating without executing\n"
	ls -ltrha ${runningFile}
	if [ `find ${currPath} -maxdepth 1 -iname ".ExecutionStatus.txt" -mmin +120` ]
	then
		echo -e "\n<Error>${thisScriptName} execution was constantly terminated because of the file \" ${runningFile}\"..So removing the file \" ${runningFile}\""
		rm -rvf $runningFile
	fi
	exit 1
else
	echo "Started"  > ${runningFile}
fi


#Configure uploading scripts in the below sections
sh ${currPath}/APP_CDRUpload.sh CDR
wait

#Mandatory removal command for check of "already in execution" condition
rm -rvf ${runningFile}
echo "_ _ _ _ _ _ _ Completed CDR Uploading at `date "+%Y-%b-%d %H:%M:%S"` _ _ _ _ _ _ _"
