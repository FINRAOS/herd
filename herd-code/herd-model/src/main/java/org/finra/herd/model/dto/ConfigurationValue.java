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
package org.finra.herd.model.dto;

/**
 * An enum that lists the configuration values keys and their default values.
 */
public enum ConfigurationValue
{
    /**
     * The herd environment name.
     */
    HERD_ENVIRONMENT("herd.environment", "UNDEFINED"),

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
     * The optional herd data source JNDI name. The default is herdDB.
     */
    HERD_DATA_SOURCE_JNDI_NAME("herd.data.source.jndi.name", "java:comp/env/jdbc/herdDB"),

    /**
     * The default name of the S3 storage. The default is "S3_MANAGED".
     */
    S3_STORAGE_NAME_DEFAULT("s3.storage.name.default", "S3_MANAGED"),

    /**
     * The default storage name for external storage for use with LFU.
     */
    S3_EXTERNAL_STORAGE_NAME_DEFAULT("s3.external.storage.name.default", "S3_MANAGED_EXTERNAL"),

    /**
     * The S3 attribute name for bucket name. The default is "bucket.name".
     */
    S3_ATTRIBUTE_NAME_BUCKET_NAME("s3.attribute.name.bucket.name", "bucket.name"),

    /**
     * The S3 attribute name for validating the path prefix. The default is "validate.path.prefix".
     */
    S3_ATTRIBUTE_NAME_VALIDATE_PATH_PREFIX("s3.attribute.name.validate.path.prefix", "validate.path.prefix"),

    /**
     * The S3 attribute name for validating the file existence. The default is "validate.file.existence".
     */
    S3_ATTRIBUTE_NAME_VALIDATE_FILE_EXISTENCE("s3.attribute.name.validate.file.existence", "validate.file.existence"),

    /**
     * The S3 attribute name for validating the file size. The default is "validate.file.size".
     */
    S3_ATTRIBUTE_NAME_VALIDATE_FILE_SIZE("s3.attribute.name.validate.file.size", "validate.file.size"),

    /**
     * The storage attribute name which specifies the upload role ARN.
     */
    S3_ATTRIBUTE_NAME_UPLOAD_ROLE_ARN("s3.attribute.name.upload.role.arn", "upload.role.arn"),

    /**
     * The storage attribute name which specifies the upload session duration in seconds.
     */
    S3_ATTRIBUTE_NAME_UPLOAD_SESSION_DURATION_SECS("s3.attribute.name.upload.session.duration.secs", "upload.session.duration.secs"),

    /**
     * The storage attribute name which specifies the download role ARN.
     */
    S3_ATTRIBUTE_NAME_DOWNLOAD_ROLE_ARN("s3.attribute.name.download.role.arn", "download.role.arn"),

    /**
     * The storage attribute name which specifies the download session duration in seconds.
     */
    S3_ATTRIBUTE_NAME_DOWNLOAD_SESSION_DURATION_SECS("s3.attribute.name.download.session.duration.secs", "download.session.duration.secs"),

    /**
     * The storage attribute name which specifies the KMS key ID to use to encrypt/decrypt objects in the bucket.
     */
    S3_ATTRIBUTE_NAME_KMS_KEY_ID("s3.attribute.name.kms.key.id", "kms.key.id"),

    /**
     * The storage attribute name which specifies the S3 key prefix velocity template.
     */
    S3_ATTRIBUTE_NAME_KEY_PREFIX_VELOCITY_TEMPLATE("s3.attribute.name.key.prefix.velocity.template", "key.prefix.velocity.template"),

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
     * The token delimiter to use for Activiti job definition ID template. The default is the tilde character.
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
     * The cron expression to schedule "ec2OnDemandPricingUpdate" system job. Default is to disable the job by scheduling it to run way in the future.
     */
    EC2_ON_DEMAND_PRICING_UPDATE_JOB_CRON_EXPRESSION("ec2.on.demand.pricing.update.job.cron.expression", "59 59 23 31 12 ? 2099"),

    /**
     * The URL of the Amazon EC2 pricing list in JSON format. Default is set to the URL specified by Amazon.
     */
    EC2_ON_DEMAND_PRICING_UPDATE_JOB_EC2_PRICING_LIST_URL("ec2.on.demand.pricing.update.job.ec2.pricing.list.url",
        "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json"),

    /**
     * The cron expression to schedule "relationalTableSchemaUpdate" system job.  Default is to run this system job every night at 8 AM.
     */
    RELATIONAL_TABLE_SCHEMA_UPDATE_JOB_CRON_EXPRESSION("relational.table.schema.update.job.cron.expression", "0 0 8 * * ?"),

    /**
     * The cron expression to schedule "storagePolicySelector" system job.  Default is to run the system job every night at 2 AM.
     */
    STORAGE_POLICY_SELECTOR_JOB_CRON_EXPRESSION("storage.policy.selector.job.cron.expression", "0 0 2 * * ?"),

    /**
     * AWS SQS queue name where storage policy selector job sends storage policy selection messages.
     */
    STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME("storage.policy.selector.job.sqs.queue.name", null),

    /**
     * AWS SQS queue name where sample data upload sends message
     */
    SAMPLE_DATA_SQS_QUEUE_NAME("sample.data.sqs.queue.name", null),

    /**
     * The maximum number of business object data instances to be selected per storage policies in a single run of the storage policy selector system job. The
     * default is 1000 business object data instances.
     */
    STORAGE_POLICY_SELECTOR_JOB_MAX_BDATA_INSTANCES("storage.policy.selector.job.max.business.object.data.instances", "1000"),

    /**
     * The maximum number of failed storage policy transition attempts before the relative storage unit gets excluded from being selected per storage policies
     * by the storage policy selector system job. 0 means the maximum is not set. The default is 3.
     */
    STORAGE_POLICY_TRANSITION_MAX_ALLOWED_ATTEMPTS("storage.policy.transition.max.allowed.attempts", 3),

    /**
     * The threshold in days since business object data registration update for business object data to be selectable by a storage policy of the
     * DAYS_SINCE_BDATA_PRIMARY_PARTITION_VALUE storage policy rule type. The default is 90 days.
     */
    STORAGE_POLICY_PROCESSOR_BDATA_UPDATED_ON_THRESHOLD_DAYS("storage.policy.processor.business.object.data.updated.on.threshold.days", 90),

    /**
     * The cron expression to schedule "businessObjectDataFinalizeRestore" system job. Default is to run the system job every 6 hours.
     */
    BDATA_FINALIZE_RESTORE_JOB_CRON_EXPRESSION("business.object.data.finalize.restore.job.cron.expression", "0 0 0/6 * * ?"),

    /**
     * The maximum number of business object data instances being restored that can get processed in a single run of this system job.  The default is 1000
     * business object data instances.
     */
    BDATA_FINALIZE_RESTORE_JOB_MAX_BDATA_INSTANCES("business.object.data.finalize.restore.job.max.business.object.data.instances", "1000"),

    /**
     * The cron expression to schedule "businessObjectDataFinalizeRestore" system job. The default is to run this system job every 6 hours every day, starting
     * at 1 AM.
     */
    EXPIRE_RESTORED_BDATA_JOB_CRON_EXPRESSION("expire.restored.business.object.data.job.cron.expression", "0 0 1/6 * * ?"),

    /**
     * The maximum number of business object data instances with expired restoration interval that can get processed in a single run of this system job. The
     * default is 1000 business object data instances.
     */
    EXPIRE_RESTORED_BDATA_JOB_MAX_BDATA_INSTANCES("expire.restored.business.object.data.job.max.business.object.data.instances", "1000"),

    /**
     * The cron expression to schedule "cleanupDestroyedBusinessObjectData" system job. The default is to run this system job every 6 hours every day, starting
     * at 3 AM.
     */
    CLEANUP_DESTROYED_BDATA_JOB_CRON_EXPRESSION("cleanup.destroyed.business.object.data.job.cron.expression", "0 0 3/6 * * ?"),

    /**
     * The maximum number of business object data instances with expired restoration interval that can get processed in a single run of this system job. The
     * default is 1000 business object data instances.
     */
    CLEANUP_DESTROYED_BDATA_JOB_MAX_BDATA_INSTANCES("cleanup.destroyed.business.object.data.job.max.business.object.data.instances", "1000"),

    /**
     * The default value for the expiration time for the business object data restore. The default is 30 days
     */
    BDATA_RESTORE_EXPIRATION_IN_DAYS_DEFAULT("business.object.data.restore.expiration.in.days.default", 30),

    /**
     * The delay time in days to complete the business object data destroy operation. The default is 15 days
     */
    BDATA_FINAL_DESTROY_DELAY_IN_DAYS("business.object.data.final.destroy.delay.in.days", 15),

    /**
     * The tokenized template of the Activiti Id. The default is computed dynamically so it is not listed here.
     */
    ACTIVITI_JOB_DEFINITION_ID_TEMPLATE("activiti.job.definition.id.template", null),

    /**
     * The default "from" field for Activiti mail task
     */
    ACTIVITI_DEFAULT_MAIL_FROM("activiti.default.mail.from", null),

    /**
     * Asserts when the first task in the activiti workflow is asynchronous when the value is true.
     */
    ACTIVITI_JOB_DEFINITION_ASSERT_ASYNC("activiti.job.definition.assert.async", true),

    /**
     * The maximum number of results that will be returned in a jobs query. The default is 1000 results.
     */
    JOBS_QUERY_MAX_RESULTS("jobs.query.max.results", 1000),

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
     * Bootstrapping script for daemon configuration. The default value is the path to the EMR configure daemons script.
     */
    EMR_CONFIGURE_DAEMON("emr.aws.configure.daemon", "s3://elasticmapreduce/bootstrap-actions/configure-daemons"),

    /**
     * The list of product descriptions to filter by when looking up EMR spot price history.
     */
    EMR_SPOT_PRICE_HISTORY_PRODUCT_DESCRIPTIONS("emr.spot.price.history.product.descriptions", null),

    /**
     * The threshold value in percentage when choosing an EMR cluster based on the lowest core instance price. It should be of string type so the precision can
     * be kept. The default value is 10 percent.
     */
    EMR_CLUSTER_LOWEST_CORE_INSTANCE_PRICE_PERCENTAGE("emr.cluster.lowest.core.instance.price.threshold.percentage", "0.1"),

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
     * The mandatory AWS tags for instances. This is required so there is no default.
     */
    MANDATORY_AWS_TAGS("mandatory.aws.tags", null),

    /**
     * The number of times AWS SDK clients will retry on error before giving up.
     */
    AWS_MAX_RETRY_ATTEMPT("aws.max.retry.attempt", 8),

    /**
     * The default S3 upload session duration in seconds.
     */
    AWS_S3_DEFAULT_UPLOAD_SESSION_DURATION_SECS("aws.s3.default.upload.session.duration.secs", 3600),

    /**
     * The default S3 download session duration in seconds.
     */
    AWS_S3_DEFAULT_DOWNLOAD_SESSION_DURATION_SECS("aws.s3.default.download.session.duration.secs", 3600),

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
     * The thread pool core pool size. The default is 100.
     */
    THREAD_POOL_CORE_POOL_SIZE("thread.pool.core.pool.size", 100),

    /**
     * The thread pool max pool size. The default is 100.
     */
    THREAD_POOL_MAX_POOL_SIZE("thread.pool.max.pool.size", 100),

    /**
     * The thread pool keep alive in seconds. The default is 60 seconds.
     */
    THREAD_POOL_KEEP_ALIVE_SECS("thread.pool.keep.alive.secs", 60),

    /**
     * The thread pool queue capacity.
     */
    THREAD_POOL_QUEUE_CAPACITY("thread.pool.queue.capacity", Integer.MAX_VALUE),

    /**
     * The activiti thread pool core pool size.
     */
    ACTIVITI_THREAD_POOL_CORE_POOL_SIZE("activiti.thread.pool.core.pool.size", 25),

    /**
     * The activiti thread pool max pool size.
     */
    ACTIVITI_THREAD_POOL_MAX_POOL_SIZE("activiti.thread.pool.max.pool.size", Integer.MAX_VALUE),

    /**
     * The activiti thread pool keep alive in seconds.
     */
    ACTIVITI_THREAD_POOL_KEEP_ALIVE_SECS("activiti.thread.pool.keep.alive.secs", 60),

    /**
     * The activiti thread pool queue capacity.
     */
    ACTIVITI_THREAD_POOL_QUEUE_CAPACITY("activiti.thread.pool.queue.capacity", Integer.MAX_VALUE),

    /**
     * The Activiti asynchronous job lock expiration time in milliseconds. This value should be greater than the longest running task in Activiti.
     */
    ACTIVITI_ASYNC_JOB_LOCK_TIME_MILLIS("activiti.async.job.lock.time.millis", 60 * 60 * 1000),

    /**
     * JMS listener concurrency limits via a "lower-upper" String, e.g. "5-10". Refer to DefaultMessageListenerContainer#setConcurrency for details.
     */
    JMS_LISTENER_POOL_CONCURRENCY_LIMITS("jms.listener.pool.concurrency.limits", "3-10"),

    /**
     * Indicates whether the the storage policy processor JMS message listener service is enabled or not. The default is "true" (enabled).
     */
    STORAGE_POLICY_PROCESSOR_JMS_LISTENER_ENABLED("storage.policy.processor.jms.listener.enabled", "true"),

    /**
     * Indicates whether the sample data JMS message listener service is enabled or not. The default is "true" (enabled).
     */
    SAMPLE_DATA_JMS_LISTENER_ENABLED("sample.data.jms.listener.enabled", "true"),

    /**
     * Indicates whether the JMS message listener service is enabled or not. The default is "true" (enabled).
     */
    JMS_LISTENER_ENABLED("jms.listener.enabled", "true"),

    /**
     * JMS listener concurrency limits for the storage policy processor JMS message listener service via a "lower-upper" String, e.g. "5-10". Refer to
     * DefaultMessageListenerContainer#setConcurrency for details. Default is "1-1".
     */
    STORAGE_POLICY_PROCESSOR_JMS_LISTENER_POOL_CONCURRENCY_LIMITS("storage.policy.processor.jms.listener.pool.concurrency.limits", "1-1"),

    /**
     * The maximum size in GB (gigabytes) of a business object data instance allowed to be processed (transitioned) by the storage policy processor.  The
     * default is 10 GB.
     */
    STORAGE_POLICY_PROCESSOR_BDATA_SIZE_THRESHOLD_GB("storage.policy.processor.business.object.data.size.threshold.gigabytes", 10),

    /**
     * The pagination size for the query that returns storage file paths. The default is 100000 results.
     */
    STORAGE_FILE_PATHS_QUERY_PAGINATION_SIZE("storage.file.paths.query.pagination.size", 100000),

    /**
     * The optional Log4J override configuration.
     */
    LOG4J_OVERRIDE_CONFIGURATION("log4j.override.configuration", null),

    /**
     * The optional Log4J override resource location.
     */
    LOG4J_OVERRIDE_RESOURCE_LOCATION("log4j.override.resource.location", null),

    /**
     * The herd endpoints that are not allowed.
     */
    NOT_ALLOWED_HERD_ENDPOINTS("not.allowed.herd.endpoints", null),

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
     * Indicates whether the user namespace authorization is enabled.
     */
    USER_NAMESPACE_AUTHORIZATION_ENABLED("user.namespace.authorization.enabled", "false"),

    /**
     * Indicates whether the namespace IAM role authorization is enabled
     */
    NAMESPACE_IAM_ROLE_AUTHORIZATION_ENABLED("namespace.iam.role.authorization.enabled", "false"),

    /**
     * Indicates whether the herd events are posted to AWS SQS.
     */
    HERD_NOTIFICATION_SQS_ENABLED("herd.notification.sqs.enabled", "false"),

    /**
     * AWS SQS queue name where herd events are posted to.
     */
    HERD_NOTIFICATION_SQS_OUTGOING_QUEUE_NAME("herd.notification.sqs.outgoing.queue.name", null),

    /**
     * AWS SQS queue name where herd events are posted to.
     */
    HERD_NOTIFICATION_SQS_INCOMING_QUEUE_NAME("herd.notification.sqs.incoming.queue.name", null),

    /**
     * The application name used to post message to SQS.
     */
    HERD_NOTIFICATION_SQS_APPLICATION_NAME("herd.notification.sqs.application.name", null),

    /**
     * The environment used to post message to SQS.
     */
    HERD_NOTIFICATION_SQS_ENVIRONMENT("herd.notification.sqs.environment", null),

    /**
     * The xsd used to post message to SQS.
     */
    HERD_NOTIFICATION_SQS_XSD_NAME("herd.notification.sqs.xsd.name", null),

    /**
     * A list of properties where each key is used as a key for building the system monitor response and each value is an XPath expression that will be applied
     * to the system monitor request to store values which can be used when building the response. The default value is an empty string which will produce no
     * properties.
     */
    HERD_NOTIFICATION_SQS_SYS_MONITOR_REQUEST_XPATH_PROPERTIES("herd.notification.sqs.sys.monitor.request.xpath.properties", ""),

    /**
     * The velocity template to use when generate the system monitor response. There is no default value which will cause no message to be sent.
     */
    HERD_NOTIFICATION_SQS_SYS_MONITOR_RESPONSE_VELOCITY_TEMPLATE("herd.notification.sqs.sys.monitor.response.velocity.template", null),

    /**
     * Contains a list of notification message definitions as defined in {@link org.finra.herd.model.api.xml.NotificationMessageDefinitions
     * NotificationMessageDefinitions} to use when generating notification messages for a business object data status change event. There is no default value
     * which will cause no messages to be sent.
     */
    HERD_NOTIFICATION_BUSINESS_OBJECT_DATA_STATUS_CHANGE_MESSAGE_DEFINITIONS("herd.notification.business.object.data.status.change.message.definitions", null),

    /**
     * Contains a list of notification message definitions as defined in {@link org.finra.herd.model.api.xml.NotificationMessageDefinitions
     * NotificationMessageDefinitions} to use when generating notification messages for a business object format version change event. There is no default value
     * which will cause no messages to be sent.
     */
    HERD_NOTIFICATION_BUSINESS_OBJECT_FORMAT_VERSION_CHANGE_MESSAGE_DEFINITIONS("herd.notification.business.object.format.version.change.message.definitions",
        null),

    /**
     * Contains a list of notification message definitions as defined in {@link org.finra.herd.model.api.xml.NotificationMessageDefinitions
     * NotificationMessageDefinitions} to use when generating notification messages for a storage unit status change event. There is no default value which will
     * cause no messages to be sent.
     */
    HERD_NOTIFICATION_STORAGE_UNIT_STATUS_CHANGE_MESSAGE_DEFINITIONS("herd.notification.storage.unit.status.change.message.definitions", null),

    /**
     * Contains a list of notification message definitions as defined in {@link org.finra.herd.model.api.xml.NotificationMessageDefinitions
     * NotificationMessageDefinitions} to use when generating notification messages for a business object definition descriptive suggestion change event. There
     * is no default value which will cause no messages to be sent.
     */
    HERD_NOTIFICATION_BUSINESS_OBJECT_DEFINITION_DESCRIPTION_SUGGESTION_CHANGE_MESSAGE_DEFINITIONS(
        "herd.notification.business.object.definition.description.suggestion.change.message.definitions", null),

    /**
     * The cache time to live in seconds defined in net.sf.ehcache.config.CacheConfiguration.
     */
    HERD_CACHE_TIME_TO_LIVE_SECONDS("herd.cache.time.to.live.seconds", 300L),

    /**
     * The cache time to idle in seconds defined in net.sf.ehcache.config.CacheConfiguration.
     */
    HERD_CACHE_TIME_TO_IDLE_SECONDS("herd.cache.time.to.idle.seconds", 0L),

    /**
     * The max elements in cache memory defined in net.sf.ehcache.config.CacheConfiguration.
     */
    HERD_CACHE_MAX_ELEMENTS_IN_MEMORY("herd.cache.max.elements.in.memory", 10000),

    /**
     * The cache memory store eviction policy defined in net.sf.ehcache.config.CacheConfiguration.
     */
    HERD_CACHE_MEMORY_STORE_EVICTION_POLICY("herd.cache.memory.store.eviction.policy", "LRU"),

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
    JDBC_RESULT_MAX_ROWS("jdbc.result.max.rows", null),

    /**
     * The maximum number of records returned in business object data search results
     */
    BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULTS("business.object.data.search.max.results", 1000),

    /**
     * The maximum number of records returned in business object data search result count
     */
    BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULT_COUNT("business.object.data.search.max.result.count", 10_000),

    /**
     * The maximum number of records returned in business object data search page
     */
    BUSINESS_OBJECT_DATA_SEARCH_MAX_PAGE_SIZE("business.object.data.search.max.page.size", 1_000),

    /**
     * The maximum number of nested tags allowed
     */
    MAX_ALLOWED_TAG_NESTING("tag.max.nesting", 10),

    /**
     * The cut-off length of the short description
     */
    BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH("business.object.definition.short.description.max.length", 300),

    /**
     * The url of the LDAP server. Utility method if only one server is used.
     */
    LDAP_URL("ldap.url", null),

    /**
     * The base suffix from which all LDAP operations should origin. If a base suffix is set, you will not have to (and, indeed, must not) specify the full
     * distinguished names in any operations performed.
     */
    LDAP_BASE("ldap.base", null),

    /**
     * The LDAP user distinguished name (principal) to use for getting authenticated contexts.
     */
    LDAP_USER_DN("ldap.user.dn", ""),

    /**
     * The LDAP password (credentials) in base64 encoded format to use for getting authenticated contexts. If this value is not configured, the application
     * falls back to "ldap.password" configuration option.
     */
    LDAP_PASSWORD_BASE64("ldap.password.base64", ""),

    /**
     * The LDAP password (credentials) to use for getting authenticated contexts. This configuration option is ignored when "ldap.password.base64" is
     * configured.
     */
    LDAP_PASSWORD("ldap.password", ""),

    /**
     * The LDAP attribute id for user's fully qualified username.
     */
    LDAP_ATTRIBUTE_USER_ID("ldap.attribute.user.id", "userPrincipalName"),

    /**
     * The LDAP attribute id for user's full name.
     */
    LDAP_ATTRIBUTE_USER_FULL_NAME("ldap.attribute.user.full.name", "name"),

    /**
     * The LDAP attribute id for user's job title.
     */
    LDAP_ATTRIBUTE_USER_JOB_TITLE("ldap.attribute.user.job.title", "title"),

    /**
     * The LDAP attribute id for user's e-mail address.
     */
    LDAP_ATTRIBUTE_USER_EMAIL_ADDRESS("ldap.attribute.user.email.address", "mail"),

    /**
     * The LDAP attribute id for user's telephone number.
     */
    LDAP_ATTRIBUTE_USER_TELEPHONE_NUMBER("ldap.attribute.user.telephone.number", "telephoneNumber"),

    /**
     * The elasticsearch index name
     */
    ELASTICSEARCH_BDEF_INDEX_NAME("elasticsearch.bdef.index.name", "bdef"),

    /**
     * The elasticsearch index name
     */
    ELASTICSEARCH_TAG_INDEX_NAME("elasticsearch.tag.index.name", "tag"),

    /**
     * The elasticsearch document type
     */
    ELASTICSEARCH_BDEF_DOCUMENT_TYPE("elasticsearch.bdef.document.type", "doc"),

    /**
     * The elasticsearch document type
     */
    ELASTICSEARCH_TAG_DOCUMENT_TYPE("elasticsearch.tag.document.type", "doc"),

    /**
     * The elasticsearch business object definition mappings JSON
     */
    ELASTICSEARCH_BDEF_MAPPINGS_JSON("elasticsearch.bdef.mappings.json", "{\"properties\": { \"id\": { \"type\": \"long\" } } }"),

    /**
     * The elasticsearch business object definition settings JSON
     */
    ELASTICSEARCH_BDEF_SETTINGS_JSON("elasticsearch.bdef.settings.json",
        "{\"analysis\":{\"filter\":{\"field_ngram_filter\":{\"type\":\"edgeNGram\",\"min_gram\":1,\"max_gram\":16,\"side\":\"front\"}}}}"),

    /**
     * The elasticsearch tag mappings JSON
     */
    ELASTICSEARCH_TAG_MAPPINGS_JSON("elasticsearch.tag.mappings.json", "{\"properties\": { \"id\": { \"type\": \"long\" } } }"),

    /**
     * The elasticsearch tag settings JSON
     */
    ELASTICSEARCH_TAG_SETTINGS_JSON("elasticsearch.tag.settings.json",
        "{\"analysis\":{\"filter\":{\"field_ngram_filter\":{\"type\":\"edgeNGram\",\"min_gram\":1,\"max_gram\":16,\"side\":\"front\"}}}}"),

    /**
     * The elasticsearch settings JSON
     */
    ELASTICSEARCH_SETTINGS_JSON("elasticsearch.settings.json",
        "{ \"clientTransportAddresses\": [\"localhost\"], \"clientTransportSniff\": true, \"elasticSearchCluster\": \"elasticsearch\" }"),

    /**
     * Searchable 'stemmed' fields, defaults to all stemmed fields with no boost
     */
    ELASTICSEARCH_SEARCHABLE_FIELDS_STEMMED("elasticsearch.searchable.fields.stemmed", "{\"*.stemmed\": \"1.0\"}"),

    /**
     * Searchable 'stemmed' fields, defaults to all ngrams fields with no boost
     */
    ELASTICSEARCH_SEARCHABLE_FIELDS_NGRAMS("elasticsearch.searchable.fields.ngrams", "{\"*.ngrams\": \"1.0\"}"),

    /**
     * Searchable 'shingles' fields, defaults to all shingles fields with no boost
     */
    ELASTICSEARCH_SEARCHABLE_FIELDS_SHINGLES("elasticsearch.searchable.fields.shingles", "{\"*.shingles\": \"1.0\"}"),

    /**
     * Phrase prefix query boost value
     */
    ELASTICSEARCH_PHRASE_PREFIX_QUERY_BOOST("elasticsearch.phrase.prefix.query.boost", 1.0f),

    /**
     * Phrase query boost value
     */
    ELASTICSEARCH_PHRASE_QUERY_BOOST("elasticsearch.phrase.query.boost", 1.0f),

    /**
     * Phrase query slop value
     */
    ELASTICSEARCH_PHRASE_QUERY_SLOP("elasticsearch.phrase.query.slop", 6),

    /**
     * Best fields query boost value
     */
    ELASTICSEARCH_BEST_FIELDS_QUERY_BOOST("elasticsearch.best.fields.query.boost", 1.0f),

    /**
     * Pre-tags used for highlighting
     */
    ELASTICSEARCH_HIGHLIGHT_PRETAGS("elasticsearch.highlight.pretags", "<hlt class=\"highlight\">"),

    /**
     * Post-tags used for highlighting
     */
    ELASTICSEARCH_HIGHLIGHT_POSTTAGS("elasticsearch.highlight.posttags", "</hlt>"),

    /**
     * Fields on which highlighting should be done, defaults to all fields
     */
    ELASTICSEARCH_HIGHLIGHT_FIELDS("elasticsearch.highlight.fields", "{\"fields\": [\"*\"]}"),

    /**
     * Fields on which highlighting should be done when the column match is used
     */
    ELASTICSEARCH_COLUMN_MATCH_HIGHLIGHT_FIELDS("elasticsearch.column.match.highlight.fields", "{\"fields\": [\"*\"]}"),

    /**
     * The elasticsearch default port
     */
    ELASTICSEARCH_DEFAULT_PORT("elasticsearch.default.port", 9300),

    /**
     * The elasticsearch spot check percentage for bdefs
     */
    ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE("elasticsearch.bdef.spot.check.percentage", 0.05),

    /**
     * The elasticsearch spot check most recent number for bdefs
     */
    ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER("elasticsearch.bdef.spot.check.most.recent.number", 100),

    /**
     * The elasticsearch spot check percentage for tags
     */
    ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE("elasticsearch.tag.spot.check.percentage", 0.2),

    /**
     * The elasticsearch spot check most recent number for tags
     */
    ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER("elasticsearch.tag.spot.check.most.recent.number", 10),

    /**
     * The elasticsearch search rest client hostname
     */
    ELASTICSEARCH_REST_CLIENT_HOSTNAME("elasticsearch.rest.client.hostname", "localhost"),

    /**
     * The elasticsearch search rest client port number
     */
    ELASTICSEARCH_REST_CLIENT_PORT("elasticsearch.rest.client.port", 9200),

    /**
     * The elasticsearch search rest client scheme
     */
    ELASTICSEARCH_REST_CLIENT_SCHEME("elasticsearch.rest.client.scheme", "http"),

    /*
     * The elasticsearch search rest client user name
     */
    ELASTICSEARCH_REST_CLIENT_USERNAME("elasticsearch.rest.client.username", null),

    /*
     * The elasticsearch search rest client user credential name
     */
    ELASTICSEARCH_REST_CLIENT_USERCREDENTIALNAME("elasticsearch.rest.client.usercredentialname", null),

    /*
     * The elasticsearch search rest client timeout
     */
    ELASTICSEARCH_REST_CLIENT_CONNECTION_TIMEOUT("elasticsearch.rest.client.connection.timeout", 60000),

    /*
     * The elasticsearch search rest client read timeout
     */
    ELASTICSEARCH_REST_CLIENT_READ_TIMEOUT("elasticsearch.rest.client.read.timeout", 60000),

    /**
     * The search index update queue name
     */
    SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME("search.index.update.sqs.queue.name", null),

    /**
     * Indicates whether the sample data JMS message listener service is enabled or not. The default is "true" (enabled).
     */
    SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED("search.index.update.jms.listener.enabled", "true"),

    /**
     * The name of the Credstash table where credentials are stored.
     */
    CREDSTASH_TABLE_NAME("credstash.table.name", "credential-store"),

    /**
     * The Credstash encryption context key value map.
     */
    CREDSTASH_ENCRYPTION_CONTEXT("credstash.encryption.context", "{\"AGS\":\"AGS_VALUE\",\"SDLC\":\"SDLC_VALUE\",\"Component\":\"COMPONENT_VALUE\"}"),

    /**
     * The Credstash encryption context key value map for RELATIONAL storage.
     */
    CREDSTASH_RELATIONAL_STORAGE_ENCRYPTION_CONTEXT("credstash.relational.storage.encryption.context",
        "{\"AGS\":\"AGS_VALUE\",\"SDLC\":\"SDLC_VALUE\",\"Component\":\"RELATIONAL_STORAGE_COMPONENT_VALUE\"}"),

    /**
     * The Credstash aws region name.
     */
    CREDSTASH_AWS_REGION_NAME("credstash.aws.region.name", "us-east-1"),

    /**
     * The cut-off length of the short description
     */
    TAG_SHORT_DESCRIPTION_LENGTH("tag.short.description.max.length", 300),

    /**
     * The S3 object tag key to be used to trigger S3 object archiving to Glacier.
     */
    S3_ARCHIVE_TO_GLACIER_TAG_KEY("s3.archive.to.glacier.tag.key", "HerdArchiveToGlacier"),

    /**
     * The S3 object tag value to be used to trigger S3 object archiving to Glacier.
     */
    S3_ARCHIVE_TO_GLACIER_TAG_VALUE("s3.archive.to.glacier.tag.value", "true"),

    /**
     * The Amazon Resource Name (ARN) of the role to assume when tagging S3 objects to trigger archiving to Glacier.
     */
    S3_ARCHIVE_TO_GLACIER_ROLE_ARN("s3.archive.to.glacier.role.arn", null),

    /**
     * The session identifier for the assumed role to be used when tagging S3 objects to trigger archiving to Glacier.
     */
    S3_ARCHIVE_TO_GLACIER_ROLE_SESSION_NAME("s3.archive.to.glacier.role.session.name", null),

    /**
     * The S3 object tag key to be used to trigger S3 object deletion.
     */
    S3_OBJECT_DELETE_TAG_KEY("s3.object.delete.tag.key", "HerdDelete"),

    /**
     * The S3 object tag value to be used to trigger S3 object deletion.
     */
    S3_OBJECT_DELETE_TAG_VALUE("s3.object.delete.tag.value", "true"),

    /**
     * The Amazon Resource Name (ARN) of the role to assume when tagging S3 objects to trigger S3 object deletion.
     */
    S3_OBJECT_DELETE_ROLE_ARN("s3.object.delete.role.arn", null),

    /**
     * The session identifier for the assumed role to be used when tagging S3 objects to trigger S3 object deletion.
     */
    S3_OBJECT_DELETE_ROLE_SESSION_NAME("s3.object.delete.role.session.name", null),

    /**
     * The business object format attribute name for the relational database schema name. The default is "relational.schema.name".
     */
    BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME_RELATIONAL_SCHEMA_NAME("business.object.format.attribute.name.relational.schema.name", "relational.schema.name"),

    /**
     * The business object format attribute name for the relational table name. The default is "relational.table.name".
     */
    BUSINESS_OBJECT_FORMAT_ATTRIBUTE_NAME_RELATIONAL_TABLE_NAME("business.object.format.attribute.name.relational.table.name", "relational.table.name"),

    /**
     * The storage attribute name for the JDBC URL. The default is "jdbc.url".
     */
    STORAGE_ATTRIBUTE_NAME_JDBC_URL("storage.attribute.name.jdbc.url", "jdbc.url"),

    /**
     * The storage attribute name for the JDBC username. The default is "jdbc.username".
     */
    STORAGE_ATTRIBUTE_NAME_JDBC_USERNAME("storage.attribute.name.jdbc.useraname", "jdbc.username"),

    /**
     * The storage attribute name for the JDBC user credential name. The default is "jdbc.user.credential.name".
     */
    STORAGE_ATTRIBUTE_NAME_JDBC_USER_CREDENTIAL_NAME("storage.attribute.name.jdbc.user.credential.name", "jdbc.user.credential.name");

    private Object defaultValue;

    // Properties
    private String key;

    private ConfigurationValue(String key, Object defaultValue)
    {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public Object getDefaultValue()
    {
        return defaultValue;
    }

    public String getKey()
    {
        return key;
    }
}
