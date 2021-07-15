#!/bin/bash
# Copyright 2015 herd contributors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds herd 0.119 image

HERD_VER="0.119.0"

### CHANGE THESE ###
echo "**** WARNING **** CHANGE VARIABLES IN THE SCRIPT FOR YOUR S3 BUCKETS AND SQS QUEUES !!!!!"
S3BUCKET="222222:s3::::test-bucket"
INCOMING_SQSQ="22222:sqs:::::herd-incoming"
IDXUPTD_SQSQ="22222:sqs:::::herd-idxupdt"

# here to save time
if [ ! -e herd/herd-war-$HERD_VER.war ] ; then 
	curl https://oss.sonatype.org/service/local/repositories/releases/content/org/finra/herd/herd-war/$HERD_VER/herd-war-$HERD_VER.war > herd/herd-war-$HERD_VER.war
	
fi ;

if [ ! -e herd-scripts-sql-$HERD_VER.jar ] ; then 
	curl https://oss.sonatype.org/service/local/repositories/releases/content/org/finra/herd/herd-scripts-sql/$HERD_VER/herd-scripts-sql-$HERD_VER.jar > herd-scripts-sql-$HERD_VER.jar
fi ; 

if [ ! -d herd-setul-sql ] ; then
	mkdir herd-setup-sql
	cd herd-setup-sql
	unzip ../herd-scripts-sql-$HERD_VER.jar
	# fix file orders per cloudformation script
	mv herd.postgres.0.1.0.create.sql 1herd.postgres.0.1.0.create.sql
	mv herd.postgres.0.1.0.refdata.sql 2herd.postgres.0.1.0.refdata.sql
	mv herd.postgres.0.1.0.cnfgn.sql 3herd.postgres.0.1.0.cnfgn.sql
	rm herd.postgres.clean.refdata.sql

	for f in *.upgrade.sql ; do 
		mv $f 4$f ; 
	done ;

	for f in activiti*sql ; do
		mv $f 5$f ; 
	done ; 

	for f in quartz*sql ; do
		# moved to last, this borks in the current release and the initializer then stops running, so set variable
		# more scripts
		
		echo "\set ON_ERROR_STOP 0" >> 99$f
		cat $f >> 99$f
		rm -f $f
	done ; 

	for f in elasticsearch*sql ; do
		mv $f 7$f ; 
	done ;

	# manual additional data

	cat > 98last.sql << EOF

DELETE FROM cnfgn WHERE cnfgn_key_nm = 's3.managed.bucket.name';
INSERT INTO cnfgn VALUES ('s3.managed.bucket.name','${S3BUCKET}', NULL);
DELETE FROM cnfgn WHERE cnfgn_key_nm = 'herd.notification.sqs.incoming.queue.name';
INSERT INTO cnfgn VALUES ('herd.notification.sqs.incoming.queue.name','${INCOMING_SQSQ}', NULL);
DELETE FROM cnfgn WHERE cnfgn_key_nm = 'search.index.update.sqs.queue.name';
INSERT INTO cnfgn VALUES ('search.index.update.sqs.queue.name','${IDXUPTD_SQSQ}', NULL);
DELETE FROM cnfgn WHERE cnfgn_key_nm = 'security.enabled.spel.expression';
INSERT INTO cnfgn VALUES ('security.enabled.spel.expression','false', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.best.fields.query.boost','100', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.phrase.prefix.query.boost','1', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.phrase.query.boost','1000', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.rest.client.hostname','herd-elasticsearch', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.rest.client.scheme','http', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.rest.client.port','9200', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.domain.rest.client.hostname','herd-elasticsearch', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.domain.rest.client.port','9200', NULL);
INSERT INTO cnfgn VALUES ('elasticsearch.domain.rest.client.scheme','http', NULL);

EOF
fi ;
#if behind a proxy, remember --build-arg http_proxy --build-arg https_proxy

echo "You're now ready to run 'docker-compose build' if you haven't already, or re-initialize the database."

# FIXME: add uname check, this isn't needed unless it's on linux
# check sysctl value to make sure elasticsearch is going to be happy
echo "*********** WARNING ****************"
echo "Make sure vm.max_map_count = 262144 or Elasticsearch will not run. Current value is "`sysctl vm.max_map_count`