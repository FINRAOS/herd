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

myip=`hostname -i`
OOZIE_TAR_LOCATION=$1

if [ "$OOZIE_TAR_LOCATION" = "" ] ; then
	echo "Usage: $0 <OOZIE_TAR_LOCATION>"
	exit 1
fi

sudo yum install -y python-pip
sudo apt-get install -y python-pip
sudo pip install --upgrade awscli
cd /home/hadoop
wget https://s3.amazonaws.com/aws-cli/awscli-bundle.zip
unzip awscli-bundle.zip
sudo ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
execute_cmd "mkdir /tmp/init_oozie"
execute_cmd "aws s3 cp ${OOZIE_TAR_LOCATION} /tmp/init_oozie/"
execute_cmd "cat conf/core-site.xml | grep -v '</configuration>' > /tmp/core-site.xml"
execute_cmd "echo -e \"<property> \n <name>hadoop.proxyuser.hadoop.hosts</name> \n <value>${myip}</value> \n </property>\" >> /tmp/core-site.xml"
execute_cmd "echo -e '<property> \n <name>hadoop.proxyuser.hadoop.groups</name> \n <value>hadoop</value> \n </property>' >> /tmp/core-site.xml"
execute_cmd "echo -e '</configuration>' >> /tmp/core-site.xml"
execute_cmd "cp /tmp/core-site.xml /home/hadoop/conf/core-site.xml"

execute_cmd "cd /home/hadoop"
execute_cmd "tar -xvf /tmp/init_oozie/oozie-4.0.1-distro.tar"
execute_cmd "chmod -R 755 /home/hadoop/oozie-4.0.1"
execute_cmd "sed -i 's/hadoop-conf/\/home\/hadoop\/conf/g' /home/hadoop/oozie-4.0.1/conf/oozie-site.xml"
execute_cmd "/home/hadoop/oozie-4.0.1/bin/oozie-setup.sh prepare-war"
execute_cmd "/home/hadoop/oozie-4.0.1/bin/oozie-setup.sh sharelib create -fs  hdfs://$myip:9000/"
execute_cmd "/home/hadoop/oozie-4.0.1/bin/oozie-setup.sh db create -run"
execute_cmd "hadoop fs -rm -f /user/hadoop/share/lib/pig/pig-*.jar"
execute_cmd "cat /home/hadoop/hive/conf/hive-default.xml | grep -v '</configuration>' > /home/hadoop/oozie-4.0.1/conf/hive-default.xml"
execute_cmd "sed -i \"s/localhost/$myip/g\" /home/hadoop/oozie-4.0.1/conf/hive-default.xml"
execute_cmd "hadoop fs -put /home/hadoop/oozie-4.0.1/libext/mysql-connector-java-*.jar /user/hadoop/share/lib/hive"
execute_cmd "echo -e \"<property> \n <name>hive.metastore.uris</name> \n <value>thrift://${myip}:9083</value> \n </property> \n </configuration>\" >> /home/hadoop/oozie-4.0.1/conf/hive-default.xml"
execute_cmd "echo -e \"nohup /home/hadoop/hive/bin/hive --service metastore > /tmp/hive.out 2>&1 &\" > /tmp/startmetastore.sh"
hadoop fs -mkdir /user/hive
execute_cmd "hadoop fs -put /home/hadoop/oozie-4.0.1/conf/hive-default.xml /user/hive/hive-default.xml"
execute_cmd "chmod 755 /tmp/startmetastore.sh"
execute_cmd "sh -c /tmp/startmetastore.sh"
execute_cmd "/home/hadoop/oozie-4.0.1/bin/oozied.sh start"

execute_cmd "export OOZIE_HOME=/home/hadoop/oozie-4.0.1"
execute_cmd "export OOZIE_CONFIG=$OOZIE_HOME/conf"
execute_cmd "export PATH=$OOZIE_HOME/bin:$PATH"
execute_cmd "set > /home/hadoop/After-oozie-step-finished "


exit 0
