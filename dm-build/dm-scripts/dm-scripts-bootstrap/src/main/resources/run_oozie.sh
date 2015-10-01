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
if [ $# -ne 2 ] ; then
	echo "$0 <WorkflowXml> <OoziePropertiesFile>"
	exit 1
fi

export masterIp=`hostname -i`
export OOZIE_CMD=/home/hadoop/oozie-4.0.1/bin/oozie
export OOZIE_URL="http://$masterIp:11000/oozie"
workflow_xml=$1
oozie_properties=$2

execute_cmd "mkdir /tmp/oozie_run"
execute_cmd "aws s3 cp $workflow_xml /tmp/oozie_run"
execute_cmd "aws s3 cp $oozie_properties /tmp/oozie_run"
execute_cmd "hadoop fs -mkdir -p /user/hadoop/oozie_run"
execute_cmd "echo -e \"\nnameNode=hdfs://$masterIp:9000\" >> /tmp/oozie_run/job.properties"
execute_cmd "echo -e \"jobTracker=$masterIp:9022\" >> /tmp/oozie_run/job.properties"
execute_cmd "echo -e \"inputLocation=/user/hadoop/oozie_run\" >> /tmp/oozie_run/job.properties"
execute_cmd "echo -e 'oozie.wf.application.path=\${nameNode}\${inputLocation}' >> /tmp/oozie_run/job.properties"
execute_cmd "hadoop fs -put /tmp/oozie_run/* /user/hadoop/oozie_run"
execute_cmd "$OOZIE_CMD job -oozie $OOZIE_URL -config /tmp/oozie_run/job.properties -run"


exit 0
