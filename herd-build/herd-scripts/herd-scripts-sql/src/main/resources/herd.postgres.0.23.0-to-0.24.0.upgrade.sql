/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

update cnfgn
set    cnfgn_value_cl =
E'<?xml version="1.0" encoding="UTF-8" ?>
<!--
  Copyright 2015 herd contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<!-- Refresh configuration changes every 60 seconds. If this value is changed to 0, then no more monitoring will occur. -->
<Configuration status="warn" monitorInterval="60">
   <Appenders>

      <!-- Use a rolling file appender to roll over files at certain points. The main log file will be herd.log in the location specified by the environment variable. The rollover format will use the date and increment counter and archive automatically. -->
      <RollingFile name="RollingFile" fileName="${env:HERD_LOG4J_OUTPUT_BASE_PATH}logs/herd.log" filePattern="${env:HERD_LOG4J_OUTPUT_BASE_PATH}logs/herd.%d{yyyy-MM-dd}-%i.log.gz" immediateFlush="true" append="true">
         <PatternLayout pattern="%d{MMM-dd-yyyy HH:mm:ss.SSS} [%t] %-5p %c{5}.%M %X{uid} %X{activitiProcessInstanceId} - %m%n"/>
         <Policies>
            <!-- Roll over files when they reach 2GB in size or once a day, whichever comes first. -->
            <SizeBasedTriggeringPolicy size="2 GB" />
            <TimeBasedTriggeringPolicy />
         </Policies>
         <DefaultRolloverStrategy>
            <!-- Each time a rollover occurs, evaluate the following to determine when files should be deleted. Evaluate/delete files in the specified directory only. -->
            <Delete basePath="${env:HERD_LOG4J_OUTPUT_BASE_PATH}logs" maxDepth="1">
               <!-- Only consider the previously rolled over and archived files. -->
               <IfFileName glob="herd.*.log.gz">
                  <!-- Delete files after 30 days or when the accumulated size of all files exceeds 3GB. -->
                  <IfAny>
                     <IfLastModified age="30D" />
                     <IfAccumulatedFileSize exceeds="3 GB" />
                  </IfAny>
               </IfFileName>
            </Delete>
         </DefaultRolloverStrategy>
      </RollingFile>

   </Appenders>

   <Loggers>
      <!-- The default logging level for herd is "info". -->
      <Logger name="org.finra.herd" level="info" additivity="false">
          <AppenderRef ref="RollingFile"/>
      </Logger>

      <!-- Enable HTTP request logging by setting this class to debug level. -->
      <Logger name="org.finra.herd.ui.RequestLoggingFilter" level="debug" additivity="false">
          <AppenderRef ref="RollingFile"/>
      </Logger>

      <!-- Enable debug level for AWS request id information. -->
      <Logger name="com.amazonaws.requestId" level="debug" additivity="false">
          <AppenderRef ref="RollingFile"/>
      </Logger>

      <!--
       Explicitly turn off logging in certain Activiti "timer" classes so background tasks don\'t provide unnecessary error logging for "normal" situations
       such as user configured Activiti workflows with invalid classes specified, etc. We will handle the logging ourselves in our custom herd Activiti
       classes wired into the Activiti process engine configuration.
      -->
      <Logger name="org.activiti.engine.impl.jobexecutor.TimerCatchIntermediateEventJobHandler" level="off" additivity="false">
          <AppenderRef ref="RollingFile"/>
      </Logger>
      <Logger name="org.activiti.engine.impl.jobexecutor.TimerExecuteNestedActivityJobHandler" level="off" additivity="false">
          <AppenderRef ref="RollingFile"/>
      </Logger>
      <Logger name="org.activiti.engine.impl.jobexecutor.TimerStartEventJobHandler" level="off" additivity="false">
          <AppenderRef ref="RollingFile"/>
      </Logger>
      <Logger name="org.activiti.engine.impl.jobexecutor.ExecuteJobsRunnable" level="off" additivity="false">
          <AppenderRef ref="RollingFile"/>
      </Logger>

      <!-- We are using "warn" as the default level which will prevent too much logging (e.g. info, debug, etc.) unless specifically configured with a separate Logger. -->
      <Root level="warn">
         <AppenderRef ref="RollingFile"/>
      </Root>
   </Loggers>
</Configuration>'
where  cnfgn_key_nm = 'log4j.override.configuration';
