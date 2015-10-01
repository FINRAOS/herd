#!/bin/sh
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


EPHEMERAL_MNT_DIRS=$(df -h | awk '{print $NF}' | grep mnt)
export ENCR_PASSWORD=$(pwmake 512)
i=0

MYCHILDREN=""
sudo modprobe loop

for DIR in $EPHEMERAL_MNT_DIRS; do
    #
    # Setup some variables
    #
	if [ `df -h | grep $DIR | wc -l ` -gt 0 ] ; then
		ENCRYPTED_LOOPBACK_DIR=$DIR/encrypted_loopbacks
		ENCRYPTED_MOUNT_POINT=$DIR/var/lib/hadoop/dfs.encrypted/
		if [ -d "$DIR/hdfs" ] ; then
			DFS_DATA_DIR=$DIR/hdfs
		else
			DFS_DATA_DIR=$DIR/var/lib/hadoop/dfs
		fi
		ENCRYPTED_LOOPBACK_DEVICE=/dev/loop$i
		ENCRYPTED_NAME=crypt$i

		execute_cmd "mkdir -p $ENCRYPTED_LOOPBACK_DIR"
		execute_cmd "mkdir -p $ENCRYPTED_MOUNT_POINT"
		
		DISKSIZE=$(( `df  $DIR | grep $DIR | awk '{print $2}'` / 1024 / 1024 ))
		ENCRYPTED_SIZE="$((DISKSIZE-20))g"
		
		execute_cmd "sudo fallocate -l $ENCRYPTED_SIZE $ENCRYPTED_LOOPBACK_DIR/encrypted_loopback.img"
		execute_cmd "sudo chown hadoop:hadoop $ENCRYPTED_LOOPBACK_DIR/encrypted_loopback.img"
		execute_cmd "sudo losetup /dev/loop$i $ENCRYPTED_LOOPBACK_DIR/encrypted_loopback.img"
		execute_cmd "echo '$ENCR_PASSWORD' | sudo cryptsetup luksFormat -q $ENCRYPTED_LOOPBACK_DEVICE"
		execute_cmd "echo '$ENCR_PASSWORD' | sudo cryptsetup luksOpen -q  $ENCRYPTED_LOOPBACK_DEVICE $ENCRYPTED_NAME"
		
		MYCMD="sudo mkfs.ext4 -m 0 -E lazy_itable_init=1 /dev/mapper/$ENCRYPTED_NAME && sudo mount /dev/mapper/$ENCRYPTED_NAME $DFS_DATA_DIR && sudo chown hadoop:hadoop $DFS_DATA_DIR && sudo rm -rf $DFS_DATA_DIR/lost\+found && sudo echo iamdone-$ENCRYPTED_NAME && date "
		eval $MYCMD &
		MYCHILDREN="$MYCHILDREN $!"
		let i=i+1
	fi
		
done

for MYPID in $MYCHILDREN
do
    wait $MYPID
done

date
echo "everything done"
exit 0