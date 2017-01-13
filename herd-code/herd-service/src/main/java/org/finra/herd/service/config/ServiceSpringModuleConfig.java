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
package org.finra.herd.service.config;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.ClientConfiguration;
import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import org.activiti.engine.HistoryService;
import org.activiti.engine.IdentityService;
import org.activiti.engine.ManagementService;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.TaskService;
import org.activiti.engine.cfg.ProcessEngineConfigurator;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.scripting.BeansResolverFactory;
import org.activiti.engine.impl.scripting.ResolverFactory;
import org.activiti.engine.impl.scripting.ScriptBindingsFactory;
import org.activiti.engine.impl.scripting.ScriptingEngines;
import org.activiti.engine.impl.scripting.VariableScopeResolverFactory;
import org.activiti.spring.ProcessEngineFactoryBean;
import org.activiti.spring.SpringAsyncExecutor;
import org.activiti.spring.SpringCallerRunsRejectedJobsHandler;
import org.activiti.spring.SpringProcessEngineConfiguration;
import org.activiti.spring.SpringRejectedJobsHandler;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.AutowiringQuartzSpringBeanJobFactory;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchSettingsDto;
import org.finra.herd.service.activiti.HerdCommandInvoker;
import org.finra.herd.service.activiti.HerdDelegateInterceptor;
import org.finra.herd.service.activiti.HerdProcessEngineConfigurator;
import org.finra.herd.service.helper.HerdErrorInformationExceptionHandler;
import org.finra.herd.service.helper.HerdJmsDestinationResolver;
import org.finra.herd.service.systemjobs.AbstractSystemJob;
import org.finra.pet.JCredStashFX;

/**
 * Service Spring module configuration.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.herd.service",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.herd\\.service\\.config\\..*"))
public class ServiceSpringModuleConfig
{
    /**
     * The Activiti DB schema update param bean name.
     */
    public static final String ACTIVITI_DB_SCHEMA_UPDATE_PARAM_BEAN_NAME = "activitiDbSchemaUpdateParam";

    /**
     * The Create Quartz Tables bean name.
     */
    public static final String CREATE_QUARTZ_TABLES_BEAN_NAME = "createQuartzTables";

    /**
     * The Elasticsearch setting for client transport sniff
     */
    public static final String ELASTICSEARCH_SETTING_CLIENT_TRANSPORT_SNIFF = "client.transport.sniff";

    /**
     * The Elasticsearch setting for cluster name
     */
    public static final String ELASTICSEARCH_SETTING_CLUSTER_NAME = "cluster.name";

    /**
     * The Elasticsearch setting for path home
     */
    public static final String ELASTICSEARCH_SETTING_PATH_HOME = "path.home";

    /**
     * The Elasticsearch setting for path
     */
    public static final String ELASTICSEARCH_SETTING_PATH_HOME_PATH = ".";

    /**
     * The Credstash AGS index value, part of the credential string
     */
    public static final int CREDSTASH_AGS = 0;

    /**
     * The Credstash component index value, part of the credential string
     */
    public static final int CREDSTASH_COMPONENT = 1;

    /**
     * The Credstash SDLC index value, part of the credential string
     */
    public static final int CREDSTASH_SDLC = 2;

    /**
     * The Credstash credential name index value, part of the credential string
     */
    public static final int CREDSTASH_CREDENTIAL_NAME = 3;

    @Autowired
    private DataSource herdDataSource;

    @Autowired
    public PlatformTransactionManager herdTransactionManager;

    @Autowired
    public ApplicationContext applicationContext;

    @Autowired
    private HerdDelegateInterceptor herdDelegateInterceptor;

    @Autowired
    private HerdCommandInvoker herdCommandInvoker;

    @Autowired
    private HerdProcessEngineConfigurator herdProcessEngineConfigurator;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdJmsDestinationResolver herdDestinationResolver;

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Returns a new "exception handler method resolver" that knows how to resolve exception handler methods based on the "herd error information exception
     * handler". This provides the ability to return an "exception handling" method for a specific exception.
     *
     * @return the exception handler method resolver.
     */
    @Bean
    public ExceptionHandlerMethodResolver exceptionHandlerMethodResolver()
    {
        return new ExceptionHandlerMethodResolver(HerdErrorInformationExceptionHandler.class);
    }

    /**
     * Returns a Spring rejected jobs handler.
     *
     * @return the Spring rejected jobs handler.
     */
    @Bean
    public SpringRejectedJobsHandler springRejectedJobsHandler()
    {
        return new SpringCallerRunsRejectedJobsHandler();
    }

    /**
     * Activiti's dedicated TaskExecutor bean definition.
     *
     * @return TaskExecutor
     */
    @Bean
    public TaskExecutor activitiTaskExecutor()
    {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_CORE_POOL_SIZE, Integer.class));
        taskExecutor.setMaxPoolSize(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_MAX_POOL_SIZE, Integer.class));
        taskExecutor.setKeepAliveSeconds(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_KEEP_ALIVE_SECS, Integer.class));
        taskExecutor.setQueueCapacity(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_THREAD_POOL_QUEUE_CAPACITY, Integer.class));
        return taskExecutor;
    }

    /**
     * Returns an Activiti Async executor that uses our configured task executor.
     *
     * @param activitiTaskExecutor the task executor. Note that the parameter must be named as is so that Spring IoC knows which TaskExecutor bean to autowire.
     * @param springRejectedJobsHandler The Spring rejected jobs handler
     *
     * @return the Async executor.
     */
    @Bean
    public AsyncExecutor activitiAsyncExecutor(TaskExecutor activitiTaskExecutor, SpringRejectedJobsHandler springRejectedJobsHandler)
    {
        SpringAsyncExecutor activitiAsyncExecutor = new SpringAsyncExecutor(activitiTaskExecutor, springRejectedJobsHandler);
        activitiAsyncExecutor
            .setAsyncJobLockTimeInMillis(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_ASYNC_JOB_LOCK_TIME_MILLIS, Integer.class));
        return activitiAsyncExecutor;
    }

    /**
     * Gets the Activiti Process Engine Configuration.
     *
     * @param activitiAsyncExecutor the async executor to set on the configuration.
     *
     * @return the Activiti Process Engine Configuration.
     */
    @Bean
    public SpringProcessEngineConfiguration activitiProcessEngineConfiguration(AsyncExecutor activitiAsyncExecutor)
    {
        // Initialize a new process engine configuration for Activiti that is Spring enabled.
        SpringProcessEngineConfiguration configuration = new SpringProcessEngineConfiguration();

        // Share the herd data source and transaction manager with Activiti so all DB operations between the herd schema and the Activiti schema will occur
        // within the same transaction that can be committed or rolled back together.
        configuration.setDataSource(herdDataSource);
        configuration.setTransactionManager(herdTransactionManager);

        // Set the database schema update approach. This will be different for the main application and JUnits which is why we get it from a bean
        // via the method below.
        configuration.setDatabaseSchemaUpdate(getActivitiDbSchemaUpdateParamBeanName());

        // Enable the async executor so threads can be picked up and worked on.
        configuration.setAsyncExecutorActivate(true);

        // Explicitly wire in our "Spring" async executor which in turn is configured with our own task executor.
        configuration.setAsyncExecutorEnabled(true);
        configuration.setAsyncExecutor(activitiAsyncExecutor);

        // Set the allowed beans to an empty map so the Activiti workflows won't be able to call any arbitrary bean (e.g. services, etc.).
        configuration.setBeans(new HashMap<>());

        // Explicitly set a custom herd delegate interceptor that allows us to autowire in Spring beans onto our Java delegate tasks.
        configuration.setDelegateInterceptor(herdDelegateInterceptor);

        // Explicitly set a custom herd command invoker that allows us to perform specialized logging for asynchronous tasks.
        configuration.setCommandInvoker(herdCommandInvoker);

        // Initialize the scripting engines.
        initScriptingEngines(configuration);

        configuration.setMailServerDefaultFrom(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM));

        // Attach a custom herd process engine configurator that will allow us to modify the configuration before the engine is built.
        List<ProcessEngineConfigurator> herdConfigurators = new ArrayList<>();
        herdConfigurators.add(herdProcessEngineConfigurator);
        configuration.setConfigurators(herdConfigurators);

        // Return the configuration.
        return configuration;
    }

    /**
     * Initializes the {@link ScriptingEngines} and optionally the {@link ResolverFactory} of the given {@link ProcessEngineConfigurationImpl}. The
     * initialization logic has been copied from the protected initScriptingEngines() method in {@link ProcessEngineConfigurationImpl}. This initialization will
     * use the {@link SecuredScriptingEngines} implementation of {@link ScriptingEngines}.
     *
     * @param configuration the {@link ProcessEngineConfigurationImpl} whom {@link ScriptingEngines} will be initialized.
     */
    private void initScriptingEngines(ProcessEngineConfigurationImpl configuration)
    {
        List<ResolverFactory> resolverFactories = configuration.getResolverFactories();
        if (resolverFactories == null)
        {
            resolverFactories = new ArrayList<>();
            resolverFactories.add(new VariableScopeResolverFactory());
            resolverFactories.add(new BeansResolverFactory());
            configuration.setResolverFactories(resolverFactories);
        }
        configuration.setScriptingEngines(new SecuredScriptingEngines(new ScriptBindingsFactory(resolverFactories)));
    }

    /**
     * Gets the Activiti database schema update param from the application context statically.
     *
     * @return the Activiti database schema update param.
     */
    private String getActivitiDbSchemaUpdateParamBeanName()
    {
        return (String) ApplicationContextHolder.getApplicationContext().getBean(ACTIVITI_DB_SCHEMA_UPDATE_PARAM_BEAN_NAME);
    }

    /**
     * Gets the Activiti Process Engine.
     *
     * @param activitiProcessEngineConfiguration the Activiti process engine configuration to use.
     *
     * @return the Activiti Process Engine.
     */
    @Bean(destroyMethod = "destroy")
    public ProcessEngineFactoryBean activitiProcessEngine(SpringProcessEngineConfiguration activitiProcessEngineConfiguration)
    {
        // Create and return a factory bean that can return an Activiti process engine based on our Activiti process engine configuration bean.
        ProcessEngineFactoryBean bean = new ProcessEngineFactoryBean();
        bean.setProcessEngineConfiguration(activitiProcessEngineConfiguration);
        return bean;
    }

    @Bean
    public RepositoryService activitiRepositoryService(ProcessEngine activitiProcessEngine) throws Exception
    {
        return activitiProcessEngine.getRepositoryService();
    }

    @Bean
    public RuntimeService activitiRuntimeService(ProcessEngine activitiProcessEngine) throws Exception
    {
        return activitiProcessEngine.getRuntimeService();
    }

    @Bean
    public TaskService activitiTaskService(ProcessEngine activitiProcessEngine) throws Exception
    {
        return activitiProcessEngine.getTaskService();
    }

    @Bean
    public HistoryService activitiHistoryService(ProcessEngine activitiProcessEngine) throws Exception
    {
        return activitiProcessEngine.getHistoryService();
    }

    @Bean
    public ManagementService activitiManagementService(ProcessEngine activitiProcessEngine) throws Exception
    {
        return activitiProcessEngine.getManagementService();
    }

    @Bean
    public IdentityService activitiIdentityService(ProcessEngine activitiProcessEngine) throws Exception
    {
        return activitiProcessEngine.getIdentityService();
    }

    /**
     * Gets a Quartz scheduler factory bean that can return a Quartz scheduler.
     *
     * @return the Quartz scheduler factory bean.
     * @throws Exception if the bean couldn't be created.
     */
    @Bean
    public SchedulerFactoryBean quartzScheduler() throws Exception
    {
        SchedulerFactoryBean quartzScheduler = new SchedulerFactoryBean();

        AutowiringQuartzSpringBeanJobFactory jobFactory = new AutowiringQuartzSpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        quartzScheduler.setJobFactory(jobFactory);

        // Name our scheduler.
        quartzScheduler.setSchedulerName("herdScheduler");

        // Setup the scheduler to use Spring’s dataSource and transactionManager.
        quartzScheduler.setDataSource(herdDataSource);
        quartzScheduler.setTransactionManager(herdTransactionManager);

        // Create the Quartz tables for JUnits.
        if (shouldCreateQuartzTables())
        {
            ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator();
            resourceDatabasePopulator.addScript(new ClassPathResource("createQuartzTables.sql"));
            DatabasePopulatorUtils.execute(resourceDatabasePopulator, herdDataSource); // This is what the DataSourceInitializer does.
        }

        // Set quartz properties. Please note that Spring uses LocalDataSourceJobStore extension of JobStoreCMT.
        Properties quartzProperties = new Properties();
        quartzScheduler.setQuartzProperties(quartzProperties);

        // Configure Main Main Scheduler Properties. The "instance" parameters are needed to manage cluster instances.
        quartzProperties.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "herdSystemJobScheduler");
        quartzProperties.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID, StdSchedulerFactory.AUTO_GENERATE_INSTANCE_ID);
        quartzProperties.setProperty(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");

        // Configure ThreadPool.
        quartzProperties.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
        quartzProperties
            .setProperty("org.quartz.threadPool.threadCount", configurationHelper.getProperty(ConfigurationValue.SYSTEM_JOBS_THREAD_POOL_THREAD_COUNT));
        quartzProperties.setProperty("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread", "true");

        // Configure JobStore.
        quartzProperties.setProperty("org.quartz.jobStore.tablePrefix", "QRTZ_");
        quartzProperties.setProperty("org.quartz.jobStore.isClustered", "true");
        quartzProperties.setProperty(ConfigurationValue.QUARTZ_JOBSTORE_DRIVER_DELEGATE_CLASS.getKey(), getQuartzDatabaseDelegateClass());

        // Build a list of system jobs to be scheduled.
        Map<String, AbstractSystemJob> systemJobs = applicationContext.getBeansOfType(AbstractSystemJob.class);
        List<JobDetail> jobDetails = new ArrayList<>();
        List<CronTrigger> triggers = new ArrayList<>();
        for (Map.Entry<String, AbstractSystemJob> entry : systemJobs.entrySet())
        {
            // Prepare job detail and trigger for the system job.
            String jobName = entry.getKey();
            AbstractSystemJob systemJob = entry.getValue();
            JobDetail jobDetail = newJob(systemJob.getClass()).withIdentity(jobName).storeDurably().requestRecovery().build();
            TriggerKey jobTriggerKey = TriggerKey.triggerKey(jobName + AbstractSystemJob.CRON_TRIGGER_SUFFIX);
            CronTrigger trigger = newTrigger().withIdentity(jobTriggerKey).forJob(jobName).usingJobData(systemJob.getJobDataMap())
                .withSchedule(cronSchedule(systemJob.getCronExpression())).build();

            // Add this system job to the list of jobs/triggers to be scheduled.
            jobDetails.add(jobDetail);
            triggers.add(trigger);
        }

        // Schedule the system jobs.
        quartzScheduler.setJobDetails(jobDetails.toArray(new JobDetail[jobDetails.size()]));
        quartzScheduler.setTriggers(triggers.toArray(new CronTrigger[triggers.size()]));

        return quartzScheduler;
    }

    /**
     * Determines whether Quartz tables need to be created which should return true for JUnits only.
     *
     * @return whether Quartz tables need to be created.
     */
    private Boolean shouldCreateQuartzTables()
    {
        return (Boolean) ApplicationContextHolder.getApplicationContext().getBean(CREATE_QUARTZ_TABLES_BEAN_NAME);
    }

    /**
     * Gets a JMS listener container factory that can return a JMS listener container.
     *
     * @param jmsConnectionFactory the JMS connection factory
     *
     * @return the JMS listener container factory
     */
    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory jmsConnectionFactory)
    {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(jmsConnectionFactory);
        factory.setDestinationResolver(herdDestinationResolver);
        factory.setConcurrency(configurationHelper.getProperty(ConfigurationValue.JMS_LISTENER_POOL_CONCURRENCY_LIMITS));

        return factory;
    }

    /**
     * Gets a JMS listener container factory that returns a JMS listener container for the storage policy processor JMS message listener service.
     *
     * @param jmsConnectionFactory the JMS connection factory
     *
     * @return the JMS listener container factory
     */
    @Bean
    public DefaultJmsListenerContainerFactory storagePolicyProcessorJmsListenerContainerFactory(ConnectionFactory jmsConnectionFactory)
    {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(jmsConnectionFactory);
        factory.setDestinationResolver(herdDestinationResolver);
        factory.setConcurrency(configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_PROCESSOR_JMS_LISTENER_POOL_CONCURRENCY_LIMITS));

        return factory;
    }

    /**
     * Gets a JMS connection factory.
     *
     * @return the JMS connection factory.
     */
    @Bean
    public ConnectionFactory jmsConnectionFactory()
    {
        AwsParamsDto awsParamsDto = awsHelper.getAwsParamsDto();

        ClientConfiguration clientConfiguration = new ClientConfiguration();

        // Only set the proxy hostname and/or port if they're configured.
        if (StringUtils.isNotBlank(awsParamsDto.getHttpProxyHost()))
        {
            clientConfiguration.setProxyHost(awsParamsDto.getHttpProxyHost());
        }
        if (awsParamsDto.getHttpProxyPort() != null)
        {
            clientConfiguration.setProxyPort(awsParamsDto.getHttpProxyPort());
        }

        return SQSConnectionFactory.builder().withClientConfiguration(clientConfiguration).build();
    }

    /**
     * Returns an elasticsearch transport client.
     *
     * @return TransportClient for the elasticsearch client is returned.
     * @throws Exception is thrown if host can not be found, or if settings object can not be mapped.
     */
    @Bean
    public TransportClient transportClient() throws Exception
    {
        // Get the elasticsearch settings JSON string from the configuration
        String elasticSearchSettingsJSON = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON);
        Integer port = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class);

        // Map the JSON object to the elastic search setting data transfer object
        ElasticsearchSettingsDto elasticsearchSettingsDto = jsonHelper.unmarshallJsonToObject(ElasticsearchSettingsDto.class, elasticSearchSettingsJSON);

        // Get the settings from the elasticsearch settings data transfer object
        String elasticSearchCluster = elasticsearchSettingsDto.getElasticSearchCluster();
        List<String> elasticSearchAddresses = elasticsearchSettingsDto.getClientTransportAddresses();
        boolean clientTransportStiff = elasticsearchSettingsDto.isClientTransportSniff();
        boolean isElasticsearchSearchGuardEnabled = Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED));

        // Build the Transport client with the settings
        Settings settings;
        TransportClient transportClient;

        // If search guard is enabled then setup the keystore and truststore
        if (isElasticsearchSearchGuardEnabled)
        {
            // Get the paths to the keystore and truststore files
            String pathToKeystoreFile = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_PATH);
            String pathToTruststoreFile = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_PATH);

            // Get the credstash table name and credential names for the keystore and truststore
            String credstashTableName = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
            String keystoreCredentialName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_CREDENTIAL_NAME);
            String truststoreCredentialName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_CREDENTIAL_NAME);

            // Split on a period because the credential names are in the form: AGS.component.sdlc.credentialName
            String[] keystoreCredentialNameSplit = keystoreCredentialName.split("\\.");
            String[] truststoreCredentialNameSplit = truststoreCredentialName.split("\\.");

            // Get the keystore and truststore passwords from Credstash
            JCredStashFX credstash = new JCredStashFX();

            String keystorePassword = credstash.getCredential(
                keystoreCredentialNameSplit[CREDSTASH_CREDENTIAL_NAME],
                keystoreCredentialNameSplit[CREDSTASH_AGS],
                keystoreCredentialNameSplit[CREDSTASH_SDLC],
                keystoreCredentialNameSplit[CREDSTASH_COMPONENT],
                credstashTableName);

            String truststorePassword = credstash.getCredential(
                truststoreCredentialNameSplit[CREDSTASH_CREDENTIAL_NAME],
                truststoreCredentialNameSplit[CREDSTASH_AGS],
                truststoreCredentialNameSplit[CREDSTASH_SDLC],
                truststoreCredentialNameSplit[CREDSTASH_COMPONENT],
                credstashTableName);

            // Build the settings for the transport client
            settings = Settings.builder()
                .put(ELASTICSEARCH_SETTING_CLIENT_TRANSPORT_SNIFF, clientTransportStiff)
                .put(ELASTICSEARCH_SETTING_CLUSTER_NAME, elasticSearchCluster)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, new File(pathToKeystoreFile))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, new File(pathToTruststoreFile))
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, keystorePassword)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, truststorePassword)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                .put(ELASTICSEARCH_SETTING_PATH_HOME, ELASTICSEARCH_SETTING_PATH_HOME_PATH)
                .build();

            // Build the Transport client with the settings
            transportClient = new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class);
        }
        else
        {
            // Build the settings for the transport client
            settings = Settings.builder()
                .put(ELASTICSEARCH_SETTING_CLIENT_TRANSPORT_SNIFF, clientTransportStiff)
                .put(ELASTICSEARCH_SETTING_CLUSTER_NAME, elasticSearchCluster)
                .build();

            // Build the Transport client with the settings
            transportClient = new PreBuiltTransportClient(settings);
        }

        // For each elastic search address in the elastic search address list
        for (String elasticSearchAddress : elasticSearchAddresses)
        {
            // Add the address to the transport client
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticSearchAddress), port));
        }

        return transportClient;
    }

    /**
     * The quartz driver delegate class, that works with the quartz database. Throws {@link IllegalStateException} if undefined.
     *
     * @return the quartz driver delegate class.
     */
    private String getQuartzDatabaseDelegateClass()
    {
        String quartzDelegateClass = configurationHelper.getProperty(ConfigurationValue.QUARTZ_JOBSTORE_DRIVER_DELEGATE_CLASS);

        if (StringUtils.isBlank(quartzDelegateClass))
        {
            throw new IllegalStateException(String.format("Quartz driver delegate class name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.QUARTZ_JOBSTORE_DRIVER_DELEGATE_CLASS.getKey()));
        }

        return quartzDelegateClass;
    }
}
