#!/bin/bash

# Define helper functions
# Check the error and fail if the last command is NOT successful
function check_error {
    return_code=${1}
    cmd="$2"
    if [[ ${return_code} -ne 0 ]]
    then
        echo "$(date "+%m/%d/%Y %H:%M:%S") *** ERROR *** ${cmd} has failed with error $return_code"
        exit 1
    fi
}

# Execute the given command and support resume option
function execute_cmd {
        cmd="${1}"
        echo ${cmd}
        eval ${cmd}
        check_error ${PIPESTATUS[0]} "$cmd"
}

# increment pom version (minor version) for all modules
echo "Updating pom version in all modules"
execute_cmd "mvn -q build-helper:parse-version versions:set -DgenerateBackupPoms=false -DnewVersion='\${parsedVersion.majorVersion}.\${parsedVersion.nextMinorVersion}.\${parsedVersion.incrementalVersion}-SNAPSHOT' 2>/dev/null"

# fetch current herd and herd-ui versions (this gets the updated version)
herd_version=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec 2>/dev/null | sed 's/-SNAPSHOT//')
herdui_next_major_version=$(( $(mvn -q -Dexec.executable=echo -Dexec.args='${herd-ui.version}' --non-recursive exec:exec 2>/dev/null | cut -d. -f2) + 1 ))

# update properties
echo "Setting herdpython version to: $herd_version"
execute_cmd "mvn -q versions:set-property -DgenerateBackupPoms=false -Dproperty=herdpython.version -DnewVersion=$herd_version"

echo "Setting herdui version to: $herdui_next_major_version"
execute_cmd "mvn -q versions:set-property -DgenerateBackupPoms=false -Dproperty=herd-ui.version -DnewVersion=0.$herdui_next_major_version.0"
