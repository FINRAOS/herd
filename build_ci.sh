#!/bin/bash

# The following runs unit tests and clover code coverage
# herd-spark inside herd-tools is ignored

echo "========================================================================"
echo "Start of build steps"
echo "========================================================================"

echo "Maven version:"
mvn -version
echo "Java version:"
java -version

cd ${WORKSPACE}/checkout || exit 1
export mvn_cmd="mvn -P herd-main -e -s ${m3_settings} -Dmaven.multiModuleProjectDirectory -Djava.awt.headless=true dependency:resolve -DargLine=-XX:MaxPermSize=768M checkstyle:check pmd:check findbugs:check clean install --batch-mode -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
echo "Running $mvn_cmd"
$mvn_cmd

if [ $? -eq 0 ]; then
    echo "INFO: maven build successful, proceeding with next steps"
else
    echo "!! ERROR !!: maven build failed, stopping !!!"
    exit 1
fi

cp ${WORKSPACE}/checkout/herd-code/herd-war/target/herd-app.war ${WORKSPACE}/release

cd ${WORKSPACE}/checkout/herd-code/herd-sdk-common/herd-sdk || exit 1
export mvn_cmd="mvn -e -s ${m3_settings} -Djava.awt.headless=true dependency:resolve -DargLine=-XX:MaxPermSize=768M checkstyle:check pmd:check findbugs:check clean install --batch-mode -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
echo "Running $mvn_cmd"
$mvn_cmd

if [ $? -eq 0 ]; then
    echo "INFO: maven build successful, proceeding with next steps"
else
    echo "!! ERROR !!: maven build failed, stopping !!!"
    exit 1
fi

cd ${WORKSPACE}/checkout/herd-code/herd-sdk-common/herd-sdk-ext || exit 1
export mvn_cmd="mvn -e -s ${m3_settings} -Djava.awt.headless=true dependency:resolve -DargLine=-XX:MaxPermSize=768M checkstyle:check pmd:check findbugs:check clean install --batch-mode -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
echo "Running $mvn_cmd"
$mvn_cmd

if [ $? -eq 0 ]; then
    echo "INFO: maven build successful, proceeding with next steps"
else
    echo "!! ERROR !!: maven build failed, stopping !!!"
    exit 1
fi

cd ${WORKSPACE}/checkout/herd-code/herd-tools || exit 1
export mvn_cmd="mvn -Ptools-main -e -s ${m3_settings} -Djava.awt.headless=true dependency:resolve -DargLine=-XX:MaxPermSize=768M checkstyle:check pmd:check findbugs:check clean install --batch-mode -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
echo "Running $mvn_cmd"
$mvn_cmd

if [ $? -eq 0 ]; then
    echo "INFO: maven build successful, proceeding with next steps"
else
    echo "!! ERROR !!: maven build failed, stopping !!!"
    exit 1
fi

cd ${WORKSPACE}/checkout/herd-code || exit 1
export mvn_cmd="mvn -e -s ${m3_settings} -Djava.awt.headless=true dependency:resolve --batch-mode -pl !herd-model-api-15 clean clover:setup test"
echo "Running $mvn_cmd"
$mvn_cmd

if [ $? -eq 0 ]; then
    echo "INFO: maven build successful, proceeding with next steps"
else
    echo "!! ERROR !!: maven build failed, stopping !!!"
    exit 1
fi

export mvn_cmd="mvn -e -s ${m3_settings} -Djava.awt.headless=true dependency:resolve --batch-mode -pl !herd-model-api-15 pre-site"
echo "Running $mvn_cmd"
$mvn_cmd

if [ $? -eq 0 ]; then
    echo "INFO: maven build successful, proceeding with next steps"
else
    echo "!! ERROR !!: maven build failed, stopping !!!"
    exit 1
fi

echo "========================================================================"
echo "End of build steps"
echo "========================================================================"
