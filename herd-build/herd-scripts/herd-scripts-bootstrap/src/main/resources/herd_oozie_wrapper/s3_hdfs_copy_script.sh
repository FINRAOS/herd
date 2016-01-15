#!/bin/bash 
#
# Copyright 2015 herd contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

function check_error {
    return_code=${1}
    if [ ${return_code} -ne 0 ]
    then
        echo "$(date "+%m/%d/%Y %H:%M:%S") *** ERROR *** ${1} ${2}"
        exit ${return_code}
    fi
}

function execute_cmd {
    cmd=${1}
	echo "$(date "+%m/%d/%Y %H:%M:%S") $cmd"
	eval $cmd
	check_error $? $cmd
}

echo "$@"
if [ $# -ne 3 ] ; then
	echo "$0 <sourceS3Location> <targetHDFSLocation> <tempFolderSuffix>"
	exit 1
fi

sourceS3Location=$1
targetHDFSLocation=$2
tempFolderSuffix=$3

#need a random number for directory
execute_cmd "mkdir /tmp/$tempFolderSuffix"
execute_cmd "aws s3 cp --recursive $sourceS3Location /tmp/$tempFolderSuffix"
execute_cmd "hadoop fs -mkdir -p $targetHDFSLocation"
execute_cmd "hadoop fs -put /tmp/$tempFolderSuffix/* $targetHDFSLocation"
execute_cmd "rm -r /tmp/$tempFolderSuffix"
exit 0
