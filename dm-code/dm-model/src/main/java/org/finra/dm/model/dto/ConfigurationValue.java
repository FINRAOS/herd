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
package org.finra.dm.model.dto;

/**
 * An enum that lists the configuration values keys and their default values.
 */
public enum ConfigurationValue
{
    /**
     * The Data Management environment name.
     */
    DM_ENVIRONMENT("dm.environment", "UNDEFINED"),

    /**
     * The hibernate dialect used for the database type. This is required so there is no default.
     */
    HIBERNATE_DIALECT("hibernate.dialect", null),

    /**
     * The database type. This is required so there is no default.
     */
    DATABASE_TYPE("org.springframework.orm.jpa.vendor.Database", null),

    /**
     * Determines whether SQL will be logged or not. Default to not showing SQL.
     */
    SHOW_SQL("hibernate.show_sql", "false"),

    /**
     * The S3 managed bucket name. This is required so there is no default.
     */
    S3_MANAGED_BUCKET_NAME("s3.managed.bucket.name", null),

    /**
     * The optional S3 endpoint to use when using S3 services. This is optional and there is no default.
     */
    S3_ENDPOINT("s3.endpoint", null),

    /**
     * The HTTP proxy hostname. This is optional and there is no default.
     */
    HTTP_PROXY_HOST("http.proxy.hostname", null),

    /**
     * The HTTP proxy port. This is optional and there is no default.
     */
    HTTP_PROXY_PORT("http.proxy.port", null),

    /**
     * The token delimiter to use for S3 key prefix templates. The default is the tilde character.
     */
    TEMPLATE_TOKEN_DELIMITER("template.token.delimiter", "~"),

    /**
     * The token delimiter to use for field data. The default is the pipe character.
     */
    FIELD_DATA_DELIMITER("field.data.delimiter", "|"),

    /**
     * The escape character used to escape field data delimiter. The default is the backslash character.
     */
    FIELD_DATA_DELIMITER_ESCAPE_CHAR("field.data.delimiter.escape.char", "\\"),

    /**
     * The S3 staging bucket name. The default is the empty string.
     */
    S3_STAGING_BUCKET_NAME("s3.staging.bucket.name", ""),

    /**
     * The S3 Staging resources location as per DB properties. The default is the empty string.
     */
    S3_STAGING_RESOURCE_BASE("s3.staging.resources.base", ""),

    /**
     * The S3 Staging resources location as per DB properties. The default is the S3 staging resource location.
     */
    S3_STAGING_RESOURCE_LOCATION("s3.staging.resources.location", "\\$\\{S3_STAGING_RESOURCE_LOCATION\\}"),

    /**
     * The tokenized template of the S3 key prefix. The default is computed dynamically so it is not listed here.
     */
    S3_KEY_PREFIX_TEMPLATE("s3.key.prefix.template", null),

    /**
     * The tokenized template of the S3 key prefix for the file upload. The default is computed dynamically so it is not listed here.
     */
    FILE_UPLOAD_S3_KEY_PREFIX_TEMPLATE("file.upload.s3.key.prefix.template", null),

    /**
     * This is the number of threads that are available for concurrent execution of system jobs. The default is 5.
     */
    SYSTEM_JOBS_THREAD_POOL_THREAD_COUNT("system.jobs.thread.pool.thread.count", "5"),

    /**
     * The quartz driver delegate class, that works with the quartz database. This is required so there is no default.
     */
    QUARTZ_JOBSTORE_DRIVER_DELEGATE_CLASS("org.quartz.jobStore.driverDelegateClass", null),

    /**
     * The cron expression to schedule "fileUploadCleanup" system job.  Default is to run the system job every night at 1 AM.
     */
    FILE_UPLOAD_CLEANUP_JOB_CRON_EXPRESSION("file.upload.cleanup.job.cron.expression", "0 0 1 * * ?"),

    /**
     * The threshold in minutes to be used to select dangling business object data S3_MANAGED_LOADING _DOCK records and orphaned multi-part upload parts for
     * deletion.  Only the dangling business object data records and all orphaned multi-part upload parts that are older than this amount of time will be
     * deleted by the job.  The default is 4320 minutes (3 days).
     */
    FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES("file.upload.cleanup.job.threshold.minutes", "4320"),

    /**
     * The cron expression to schedule "jmsPublishing" system job.  Default is to run the system job every 5 minutes.
     */
    JMS_PUBLISHING_JOB_CRON_EXPRESSION("jms.publishing.job.cron.expression", "0 0/5 * * * ?"),

    /**
     * The tokenized template of the Activiti Id. The default is computed dynamically so it is not listed here.
     */
    ACTIVITI_JOB_DEFINITION_ID_TEMPLATE("activiti.job.definition.id.template", null),

    /**
     * The node condition for running a bootstrap script. The default is whether the instance "isMaster" is true.
     */
    EMR_NODE_CONDITION("emr.aws.node.condition", "instance.isMaster=true"),

    /**
     * Valid cluster states which are considered as "Running" cluster. The default is the pipe delimited valid states.
     */
    EMR_VALID_STATES("emr.aws.valid.states", "STARTING|BOOTSTRAPPING|RUNNING|WAITING"),

    /**
     * EMR Jar file that executes a shell script. The default is the path to the EMR script runner JAR.
     */
    EMR_SHELL_SCRIPT_JAR("emr.aws.shell.script.jar", "s3://elasticmapreduce/libs/script-runner/script-runner.jar"),

    /**
     * The tokenized template of the EMR cluster name. The default is computed dynamically so it is not listed here.
     */
    EMR_CLUSTER_NAME_TEMPLATE("emr.cluster.name.template", null),

    /**
     * Bootstrapping script for encryption support. This is required so there is no default.
     */
    EMR_ENCRYPTION_SCRIPT("emr.encryption.script", null),

    /**
     * Bootstrapping script for conditional master node. The default is the path to the EMR run-if script.
     */
    EMR_CONDITIONAL_SCRIPT("emr.aws.node.conditional.script", "s3://elasticmapreduce/bootstrap-actions/run-if"),

    /**
     * Bootstrapping script for Oozie Installation. This is required so there is no default.
     */
    EMR_OOZIE_SCRIPT("emr.oozie.script", null),

    /**
     * Bootstrapping script for running oozie commands. This is required so there is no default.
     */
    EMR_OOZIE_RUN_SCRIPT("emr.oozie.run.script", null),

    /**
     * The folder on S3 where the DM wrapper workflow is. This is required so there is no default.
     */
    EMR_OOZIE_DM_WRAPPER_WORKFLOW_S3_LOCATION("emr.oozie.dm.wrapper.workflow.s3.location", null),

    /**
     * The folder on HDFS where the DM wrapper workflow is copied to.
     */
    EMR_OOZIE_DM_WRAPPER_WORKFLOW_HDFS_LOCATION("emr.oozie.dm.wrapper.workflow.hdfs.location", "/user/hadoop/datamgmt/oozie_wrapper/"),

    /**
     * S3 to HDFS copy script. This is required so there is no default.
     */
    EMR_S3_HDFS_COPY_SCRIPT("emr.s3.hdfs.copy.script", null),

    /**
     * The oozie url template.
     */
    EMR_OOZIE_URL_TEMPLATE("emr.oozie.url.template", "http://%s:11000/oozie/"),

    /**
     * The number of oozie jobs to return with EMR cluster status.
     */
    EMR_OOZIE_JOBS_TO_INCLUDE_IN_CLUSTER_STATUS("emr.oozie.jobs.to.include.in.cluster.status", 100),

    /**
     * The DM EMR support security group.
     */
    EMR_DM_SUPPORT_SECURITY_GROUP("emr.dm.support.security.group", null),

    /**
     * Bootstrapping script for daemon configuration. The default value is the path to the EMR configure daemons script.
     */
    EMR_CONFIGURE_DAEMON("emr.aws.configure.daemon", "s3://elasticmapreduce/bootstrap-actions/configure-daemons"),

    /**
     * S3 protocol for constructing an S3 URL. The default is the standard "s3" prefix.
     */
    S3_URL_PROTOCOL("s3.url.protocol", "s3://"),

    /**
     * Delimiter in the S3 URL path. This is useful while constructing the S3 key prefix during bootstrapping. The default is the forward slash character.
     */
    S3_URL_PATH_DELIMITER("s3.path.delimiter", "/"),

    /**
     * Bootstrapping script for configuring Hadoop parameters. The default value is the path to the EMR configure hadoop script.
     */
    EMR_CONFIGURE_HADOOP("emr.aws.configure.hadoop", "s3://us-east-1.elasticmapreduce/bootstrap-actions/configure-hadoop"),

    /**
     * The maximum number of instances allowed in EMR cluster. The default is 0 (i.e. no maximum).
     */
    MAX_EMR_INSTANCES_COUNT("max.emr.instance.count", 0),

    /**
     * Tar file for the installation of Oozie. This is required so there is no default.
     */
    EMR_OOZIE_TAR_FILE("emr.oozie.tar", null),

    /**
     * The mandatory AWS tags for instances. This is required so there is no default.
     */
    MANDATORY_AWS_TAGS("mandatory.aws.tags", null),

    /**
     * The minimum delay in seconds that will be waited before retrying an AWS operation. The default is 1 seconds.
     */
    AWS_MIN_RETRY_DELAY_SECS("aws.min.retry.delay.secs", 1),

    /**
     * The maximum delay in seconds that will be waited before retrying an AWS operation. The default is 60 seconds.
     */
    AWS_MAX_RETRY_DELAY_SECS("aws.max.retry.delay.secs", 60),

    /**
     * The maximum duration in seconds that a failed AWS S3 operation will be retried for. The default is 245 seconds.
     */
    AWS_S3_EXCEPTION_MAX_RETRY_DURATION_SECS("aws.s3.exception.max.retry.duration.secs", 245),

    /**
     * The maximum duration in seconds that a failed AWS EMR operation will be retried for. The default is 245 seconds.
     */
    AWS_EMR_EXCEPTION_MAX_RETRY_DURATION_SECS("aws.emr.exception.max.retry.duration.secs", 245),

    /**
     * The maximum duration in seconds that a failed AWS EC2 operation will be retried for. The default is 245 seconds.
     */
    AWS_EC2_EXCEPTION_MAX_RETRY_DURATION_SECS("aws.ec2.exception.max.retry.duration.secs", 245),

    /**
     * The maximum duration in seconds that a failed AWS STS operation will be retried for. The default is 245 seconds.
     */
    AWS_STS_EXCEPTION_MAX_RETRY_DURATION_SECS("aws.sts.exception.max.retry.duration.secs", 245),

    /**
     * The maximum duration in seconds that a failed AWS SQS operation will be retried for. The default is 245 seconds.
     */
    AWS_SQS_EXCEPTION_MAX_RETRY_DURATION_SECS("aws.sqs.exception.max.retry.duration.secs", 245),

    /**
     * The error codes in AmazonServiceException that we re-try on for S3 operations.
     */
    AWS_S3_RETRY_ON_ERROR_CODES("aws.s3.retry.on.error.codes", null),

    /**
     * The error codes in AmazonServiceException that we re-try on for EMR operations.
     */
    AWS_EMR_RETRY_ON_ERROR_CODES("aws.emr.retry.on.error.codes", null),

    /**
     * The error codes in AmazonServiceException that we re-try on for EC2 operations.
     */
    AWS_EC2_RETRY_ON_ERROR_CODES("aws.ec2.retry.on.error.codes", null),

    /**
     * The error codes in AmazonServiceException that we re-try on for STS operations.
     */
    AWS_STS_RETRY_ON_ERROR_CODES("aws.sts.retry.on.error.codes", null),

    /**
     * The error codes in AmazonServiceException that we re-try on for SQS operations.
     */
    AWS_SQS_RETRY_ON_ERROR_CODES("aws.sqs.retry.on.error.codes", null),

    /**
     * The Amazon Resource Name (ARN) of the role that is required to provide access to the S3_MANAGED_LOADING_DOCK storage. This is required so there is no
     * default.
     */
    AWS_LOADING_DOCK_UPLOADER_ROLE_ARN("aws.loading.dock.uploader.role.arn", null),

    /**
     * The Amazon Resource Name (ARN) of the role that is required to provide access to the S3_MANAGED_EXTERNAL storage. This is required so there is no
     * default.
     */
    AWS_EXTERNAL_DOWNLOADER_ROLE_ARN("aws.external.downloader.role.arn", null),

    /**
     * The duration, in seconds, of the role session to access the S3_MANAGED_LOADING_DOCK storage. The value can range from 900 seconds (15 minutes) to 3600
     * seconds (1 hour). The default is 900 (15 minutes).
     */
    AWS_LOADING_DOCK_UPLOADER_ROLE_DURATION_SECS("aws.loading.dock.uploader.role.duration.secs", 900),

    /**
     * The duration, in seconds, of the role session to access the S3_MANAGED_EXTERNAL storage. The value can range from 900 seconds (15 minutes) to 3600
     * seconds (1 hour). The default is 3600 (1 hour).
     */
    AWS_EXTERNAL_DOWNLOADER_ROLE_DURATION_SECS("aws.external.downloader.role.duration.secs", 3600),

    /**
     * AWS KMS Key ID for the key to be used for encryption when uploading files to S3_MANAGED_LOADING_DOCK storage. This is required so there is no default.
     */
    AWS_KMS_LOADING_DOCK_KEY_ID("aws.kms.loading.dock.key.id", null),

    /**
     * AWS KMS Key ID for the key to be used for encryption when copying files to S3_MANAGED_EXTERNAL storage. This is required so there is no default.
     */
    AWS_KMS_EXTERNAL_KEY_ID("aws.kms.external.key.id", null),

    /**
     * The optional maximum number of expected partition values allowed for availability and DDL generation. If not specified, any number of partition values is
     * allowed.
     */
    AVAILABILITY_DDL_MAX_PARTITION_VALUES("availability.ddl.max.partition.values", null),

    /**
     * The chunk size to use when creating database "in" clauses. The default chunk size to use for "in" clauses is 1000. For Oracle specifically, "in" clauses
     * can't be greater than 1000 or a SQL error will be thrown.
     */
    DB_IN_CLAUSE_CHUNK_SIZE("db.in.clause.chunk.size", 1000),

    /**
     * The thread pool core pool size. The default is 5.
     */
    THREAD_POOL_CORE_POOL_SIZE("thread.pool.core.pool.size", 5),

    /**
     * The thread pool max pool size. The default is 100.
     */
    THREAD_POOL_MAX_POOL_SIZE("thread.pool.max.pool.size", 100),

    /**
     * The thread pool keep alive in seconds. The default is 60 seconds.
     */
    THREAD_POOL_KEEP_ALIVE_SECS("thread.pool.keep.alive.secs", 60),

    /**
     * JMS listener concurrency limits via a "lower-upper" String, e.g. "5-10". Refer to DefaultMessageListenerContainer#setConcurrency for details.
     */
    JMS_LISTENER_POOL_CONCURRENCY_LIMITS("jms.listener.pool.concurrency.limits", "3-10"),

    /**
     * The optional Log4J override configuration.
     */
    LOG4J_OVERRIDE_CONFIGURATION("log4j.override.configuration", null),

    /**
     * The optional Log4J override resource location.
     */
    LOG4J_OVERRIDE_RESOURCE_LOCATION("log4j.override.resource.location", null),

    /**
     * The Log4J refresh interval in milliseconds. The default is 60000 milliseconds (i.e. 60 seconds). If 0, the read configuration will be static and not
     * refreshed.
     */
    LOG4J_REFRESH_INTERVAL_MILLIS("log4j.refresh.interval.millis", 60000L),

    /**
     * The DM endpoints that are not allowed.
     */
    NOT_ALLOWED_DM_ENDPOINTS("not.allowed.dm.endpoints", null),

    /**
     * The JAXB XML headers to use when outputting XML from the REST tier. When this isn't set, we use a default of:
     * <p/>
     * <?xml version="1.1" encoding="UTF-8" standalone="yes"?>
     * <p/>
     * To use XML 1.0, the following could be configured:
     * <p/>
     * <?xml version="1.0" encoding="UTF-8" standalone="yes"?>
     * <p/>
     * This property comes from the JAXB implementation that comes with the JDK. See com.sun.xml.bind.v2.runtime.MarshallerImpl.
     */
    JAXB_XML_HEADERS("com.sun.xml.bind.xmlHeaders", "<?xml version=\"1.1\" encoding=\"UTF-8\" standalone=\"yes\"?>"),

    /**
     * Indicates whether security is enabled. If not enabled, application will create a trusted user.
     */
    SECURITY_ENABLED_SPEL_EXPRESSION("security.enabled.spel.expression", "true"),

    /**
     * Indicates whether to use http headers based security.
     */
    SECURITY_HTTP_HEADER_ENABLED("security.http.header.enabled", "false"),

    /**
     * The http headers names for header based security.
     */
    SECURITY_HTTP_HEADER_NAMES("security.http.header.names", null),

    /**
     * Regex used to match role from a header value.
     */
    SECURITY_HTTP_HEADER_ROLE_REGEX("security.http.header.role.regex", null),

    /**
     * The regex group name to use to match a role.
     */
    SECURITY_HTTP_HEADER_ROLE_REGEX_GROUP("security.http.header.role.regex.group", null),

    /**
     * Indicates whether the data management events are posted to AWS SQS.
     */
    DM_NOTIFICATION_SQS_ENABLED("dm.notification.sqs.enabled", "false"),

    /**
     * AWS SQS queue name where data management events are posted to.
     */
    DM_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME("dm.notification.sqs.outgoing.queue.name", null),

    /**
     * AWS SQS queue name where data management events are posted to.
     */
    DM_NOTIFICATION_SQS_INCOMING_QUEUE_NAME("dm.notification.sqs.incoming.queue.name", null),

    /**
     * The application name used to post message to SQS.
     */
    DM_NOTIFICATION_SQS_APPLICATION_NAME("dm.notification.sqs.application.name", null),

    /**
     * The environment used to post message to SQS.
     */
    DM_NOTIFICATION_SQS_ENVIRONMENT("dm.notification.sqs.environment", null),

    /**
     * The xsd used to post message to SQS.
     */
    DM_NOTIFICATION_SQS_XSD_NAME("dm.notification.sqs.xsd.name", null),

    /**
     * A list of properties where each key is used as a key for building the system monitor response and each value is an XPath expression that will be applied
     * to the system monitor request to store values which can be used when building the response. The default value is an empty string which will produce no
     * properties.
     */
    DM_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES("dm.notification.sqs.sys.monitor.request.xpath.properties", ""),

    /**
     * The velocity template to use when generate the system monitor response. There is no default value which will cause no message to be sent.
     */
    DM_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE("dm.notification.sqs.sys.monitor.response.velocity.template", null),

    /**
     * The velocity template to use when generate the business object data status change message. There is no default value which will cause no message to be
     * sent.
     */
    DM_NOTIFICATION_SQS_BUSINESS_OBJECT_DATA_STATUS_CHANGE_VELOCITY_TEMPLATE("dm.notification.sqs.business.object.data.status.change.velocity.template", null),

    /**
     * The cache time to live in seconds defined in net.sf.ehcache.config.CacheConfiguration.
     */
    DM_CACHE_TIME_TO_LIVE_SECONDS("dm.cache.time.to.live.seconds", 300L),

    /**
     * The cache time to idle in seconds defined in net.sf.ehcache.config.CacheConfiguration.
     */
    DM_CACHE_TIME_TO_IDLE_SECONDS("dm.cache.time.to.idle.seconds", 0L),

    /**
     * The max elements in cache memory defined in net.sf.ehcache.config.CacheConfiguration.
     */
    DM_CACHE_MAX_ELEMENTS_IN_MEMORY("dm.cache.max.elements.in.memory", 10000),

    /**
     * The cache memory store eviction policy defined in net.sf.ehcache.config.CacheConfiguration.
     */
    DM_CACHE_MEMORY_STORE_EVICTION_POLICY("dm.cache.memory.store.eviction.policy", "LRU"),

    /**
     * The default value for EC2 node IAM profile name when creating EMR cluster.
     */
    EMR_DEFAULT_EC2_NODE_IAM_PROFILE_NAME("emr.default.ec2.node.iam.profile.name", null),

    /**
     * The default value for service IAM role name when creating EMR cluster
     */
    EMR_DEFAULT_SERVICE_IAM_ROLE_NAME("emr.default.service.iam.role.name", null),

    /**
     * The maximum number of statements allowed to be executed in JDBC service.
     */
    JDBC_MAX_STATEMENTS("jdbc.max.statements", null),

    /**
     * The maximum number of rows returned in the result of a statement execution of the JDBC service.
     */
    JDBC_RESULT_MAX_ROWS("jdbc.result.max.rows", null);

    // Properties
    private String key;
    private Object defaultValue;

    private ConfigurationValue(String key, Object defaultValue)
    {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public String getKey()
    {
        return key;
    }

    public Object getDefaultValue()
    {
        return defaultValue;
    }
}
