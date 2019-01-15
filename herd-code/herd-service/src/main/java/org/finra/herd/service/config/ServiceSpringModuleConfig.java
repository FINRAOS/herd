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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.ClientConfiguration;
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
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.AutowiringQuartzSpringBeanJobFactory;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.activiti.HerdCommandInvoker;
import org.finra.herd.service.activiti.HerdDelegateInterceptor;
import org.finra.herd.service.activiti.HerdProcessEngineConfigurator;
import org.finra.herd.service.helper.HerdErrorInformationExceptionHandler;
import org.finra.herd.service.helper.HerdJmsDestinationResolver;
import org.finra.herd.service.systemjobs.AbstractSystemJob;

/**
 * Service Spring module configuration.
 */
@EnableRetry
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.herd.service",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.herd\\.service\\.config\\..*"))
public class ServiceSpringModuleConfig
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceSpringModuleConfig.class);

    /**
     * The Activiti DB schema update param bean name.
     */
    public static final String ACTIVITI_DB_SCHEMA_UPDATE_PARAM_BEAN_NAME = "activitiDbSchemaUpdateParam";

    /**
     * The Create Quartz Tables bean name.
     */
    public static final String CREATE_QUARTZ_TABLES_BEAN_NAME = "createQuartzTables";

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

        // Set the default "from" field for Activiti mail
        String mailServerDefaultFrom = configurationHelper.getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM);
        configuration.setMailServerDefaultFrom(mailServerDefaultFrom);
        // Set the mail server hostname for Activiti mail
        String mailServerHost = configurationHelper.getProperty(ConfigurationValue.ACTIVITI_MAIL_SERVER_HOST);
        configuration.setMailServerHost(mailServerHost);
        // Set the mail server port number for Activiti mail
        int mailServerPort = configurationHelper.getProperty(ConfigurationValue.ACTIVITI_MAIL_SERVER_PORT, Integer.class);
        configuration.setMailServerPort(mailServerPort);
        LOGGER.info("Activiti mail server configurations: activitiMailServerDefaultFrom=\"{}\" activitiMailServerHost=\"{}\" activitiMailServerPort={}",
            mailServerDefaultFrom, mailServerHost, mailServerPort);

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

        // Schedule the system jobs and set a flag to overwrite already registered jobs that would be read in from the database (persistent job store).
        quartzScheduler.setOverwriteExistingJobs(true);
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
