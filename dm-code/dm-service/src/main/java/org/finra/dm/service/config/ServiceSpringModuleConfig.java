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
package org.finra.dm.service.config;

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
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.TaskService;
import org.activiti.engine.cfg.ProcessEngineConfigurator;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.jobexecutor.JobExecutor;
import org.activiti.engine.impl.scripting.BeansResolverFactory;
import org.activiti.engine.impl.scripting.ResolverFactory;
import org.activiti.engine.impl.scripting.ScriptBindingsFactory;
import org.activiti.engine.impl.scripting.ScriptingEngines;
import org.activiti.engine.impl.scripting.VariableScopeResolverFactory;
import org.activiti.spring.ProcessEngineFactoryBean;
import org.activiti.spring.SpringJobExecutor;
import org.activiti.spring.SpringProcessEngineConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;

import org.finra.dm.core.ApplicationContextHolder;
import org.finra.dm.core.AutowiringQuartzSpringBeanJobFactory;
import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.dao.helper.AwsHelper;
import org.finra.dm.model.dto.AwsParamsDto;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.service.activiti.DmCommandInvoker;
import org.finra.dm.service.activiti.DmDelegateInterceptor;
import org.finra.dm.service.activiti.DmProcessEngineConfigurator;
import org.finra.dm.service.helper.DmErrorInformationExceptionHandler;
import org.finra.dm.service.helper.DmJmsDestinationResolver;
import org.finra.dm.service.systemjobs.AbstractSystemJob;

/**
 * Service Spring module configuration.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.dm.service",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.dm\\.service\\.config\\..*") )
@Import({ServiceBasicAopSpringModuleConfig.class, ServiceAppAopSpringModuleConfig.class})
@EnableAsync
public class ServiceSpringModuleConfig implements AsyncConfigurer
{
    /**
     * The Activiti DB schema update param bean name.
     */
    public static final String ACTIVITI_DB_SCHEMA_UPDATE_PARAM_BEAN_NAME = "activitiDbSchemaUpdateParam";

    /**
     * The Create Quartz Tables bean name.
     */
    public static final String CREATE_QUARTZ_TABLES_BEAN_NAME = "createQuartzTables";

    @Autowired
    private DataSource dmDataSource;

    @Autowired
    public PlatformTransactionManager dmTransactionManager;

    @Autowired
    public ApplicationContext applicationContext;

    @Autowired
    private DmDelegateInterceptor dmDelegateInterceptor;

    @Autowired
    private DmCommandInvoker dmCommandInvoker;

    @Autowired
    private DmProcessEngineConfigurator dmProcessEngineConfigurator;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private DmJmsDestinationResolver dmDestinationResolver;

    @Autowired
    private AwsHelper awsHelper;

    /**
     * Returns a new "exception handler method resolver" that knows how to resolve exception handler methods based on the "DM error information exception
     * handler". This provides the ability to return an "exception handling" method for a specific exception.
     *
     * @return the exception handler method resolver.
     */
    @Bean
    public ExceptionHandlerMethodResolver exceptionHandlerMethodResolver()
    {
        return new ExceptionHandlerMethodResolver(DmErrorInformationExceptionHandler.class);
    }

    /**
     * Returns an Activiti job executor that uses our configured Async executor.
     *
     * @return a Spring job executor.
     */
    @Bean
    public JobExecutor jobExecutor()
    {
        return new SpringJobExecutor(getAsyncExecutor());
    }

    /**
     * Returns an Async "task" executor which is also a normal "executor". This is being wired into Activity via the job executor bean. It is also being used by
     * the "@EnableAsync" annotation and the fact that this class implements AsyncConfigurer. That way, all methods annotated with "@Async" will be executed
     * asynchronously by this executor. Thus, we have a shared thread pool that handles both Async method calls as well as Activiti asynchronous job executions
     * (e.g. timers, messages, etc.).
     *
     * @return the async task executor.
     */
    @Override
    @Bean // This will call the "initialize" method of the ThreadPoolTaskExecutor automatically.
    public TaskExecutor getAsyncExecutor()
    {
        // Create a Spring thread pool "task" executor that is backed by a JDK Thread Pool Executor.
        // Use the environment to make the key thread pool parameters configurable although changing them would require a server restart.
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(configurationHelper.getProperty(ConfigurationValue.THREAD_POOL_CORE_POOL_SIZE, Integer.class));
        executor.setMaxPoolSize(configurationHelper.getProperty(ConfigurationValue.THREAD_POOL_MAX_POOL_SIZE, Integer.class));
        executor.setKeepAliveSeconds(configurationHelper.getProperty(ConfigurationValue.THREAD_POOL_KEEP_ALIVE_SECS, Integer.class));
        return executor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler()
    {
        // In case any @Async methods return "void" and not a "Future", this exception handler will handle those cases since the caller has not way to access
        // the exception without a "Future". Just use an out-of-the-box Spring handler that logs those exceptions.
        return new SimpleAsyncUncaughtExceptionHandler();
    }

    /**
     * Gets the Activiti Process Engine Configuration.
     *
     * @return the Activiti Process Engine Configuration.
     */
    @Bean
    public SpringProcessEngineConfiguration activitiProcessEngineConfiguration()
    {
        // Initialize a new process engine configuration for Activiti that is Spring enabled.
        SpringProcessEngineConfiguration configuration = new SpringProcessEngineConfiguration();

        // Share the DM data source and transaction manager with Activiti so all DB operations between the DM schema and the Activiti schema will occur
        // within the same transaction that can be committed or rolled back together.
        configuration.setDataSource(dmDataSource);
        configuration.setTransactionManager(dmTransactionManager);

        // Set the database schema update approach. This will be different for the main application and JUnits which is why we get it from a bean
        // via the method below.
        configuration.setDatabaseSchemaUpdate(getActivitiDbSchemaUpdateParamBeanName());

        // Enable the job executor so threads can be picked up and worked on.
        configuration.setJobExecutorActivate(true);

        // Explicitly wire in our "Spring" job executor which in turn is configured with our own Async executor.
        configuration.setJobExecutor(jobExecutor());

        // Set the allowed beans to an empty map so the Activiti workflows won't be able to call any arbitrary bean (e.g. services, etc.).
        configuration.setBeans(new HashMap<>());

        // Explicitly set a custom DM delegate interceptor that allows us to autowire in Spring beans onto our Java delegate tasks.
        configuration.setDelegateInterceptor(dmDelegateInterceptor);

        // Explicitly set a custom DM command invoker that allows us to perform specialized logging for asynchronous tasks.
        configuration.setCommandInvoker(dmCommandInvoker);

        initScriptingEngines(configuration);

        // Attach a custom DM process engine configurator that will allow us to modify the configuration before the engine is built.
        List<ProcessEngineConfigurator> dmConfigurators = new ArrayList<>();
        dmConfigurators.add(dmProcessEngineConfigurator);
        configuration.setConfigurators(dmConfigurators);

        // Return the configuration.
        return configuration;
    }

    /**
     * Initializes the {@link ScriptingEngines} and optionally the {@link ResolverFactory} of the given {@link ProcessEngineConfigurationImpl}.
     * The initialization logic has been copied from the protected initScriptingEngines() method in {@link ProcessEngineConfigurationImpl}.
     * This initialization will use the {@link SecuredScriptingEngines} implementation of {@link ScriptingEngines}.
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
     * @return the Activiti Process Engine.
     */
    @Bean(destroyMethod = "destroy")
    public ProcessEngineFactoryBean activitiProcessEngine()
    {
        // Create and return a factory bean that can return an Activiti process engine based on our Activiti process engine configuration bean.
        ProcessEngineFactoryBean bean = new ProcessEngineFactoryBean();
        bean.setProcessEngineConfiguration(activitiProcessEngineConfiguration());
        return bean;
    }

    @Bean
    public RepositoryService activitiRepositoryService() throws Exception
    {
        return activitiProcessEngine().getObject().getRepositoryService();
    }

    @Bean
    public RuntimeService activitiRuntimeService() throws Exception
    {
        return activitiProcessEngine().getObject().getRuntimeService();
    }

    @Bean
    public TaskService activitiTaskService() throws Exception
    {
        return activitiProcessEngine().getObject().getTaskService();
    }

    @Bean
    public HistoryService activitiHistoryService() throws Exception
    {
        return activitiProcessEngine().getObject().getHistoryService();
    }

    @Bean
    public ManagementService activitiManagementService() throws Exception
    {
        return activitiProcessEngine().getObject().getManagementService();
    }

    @Bean
    public IdentityService activitiIdentityService() throws Exception
    {
        return activitiProcessEngine().getObject().getIdentityService();
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
        quartzScheduler.setSchedulerName("dmScheduler");

        // Setup the scheduler to use Spring’s dataSource and transactionManager.
        quartzScheduler.setDataSource(dmDataSource);
        quartzScheduler.setTransactionManager(dmTransactionManager);

        // Create the Quartz tables for JUnits.
        if (shouldCreateQuartzTables())
        {
            ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator();
            resourceDatabasePopulator.addScript(new ClassPathResource("createQuartzTables.sql"));
            DatabasePopulatorUtils.execute(resourceDatabasePopulator, dmDataSource); // This is what the DataSourceInitializer does.
        }

        // Set quartz properties. Please note that Spring uses LocalDataSourceJobStore extension of JobStoreCMT.
        Properties quartzProperties = new Properties();
        quartzScheduler.setQuartzProperties(quartzProperties);

        // Configure Main Main Scheduler Properties. The "instance" parameters are needed to manage cluster instances.
        quartzProperties.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "dmSystemJobScheduler");
        quartzProperties.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID, StdSchedulerFactory.AUTO_GENERATE_INSTANCE_ID);
        quartzProperties.setProperty(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");

        // Configure ThreadPool.
        quartzProperties.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
        quartzProperties.setProperty("org.quartz.threadPool.threadCount", configurationHelper
            .getProperty(ConfigurationValue.SYSTEM_JOBS_THREAD_POOL_THREAD_COUNT));
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
     * @param jmsConnectionFactory a JMS connection factory.
     *
     * @return the JMS listener container factory.
     */
    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(ConnectionFactory jmsConnectionFactory)
    {
        // Get configuration settings for the concurrency limits.
        String concurrencyLimits = configurationHelper.getProperty(ConfigurationValue.JMS_LISTENER_POOL_CONCURRENCY_LIMITS);

        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(jmsConnectionFactory);
        factory.setDestinationResolver(dmDestinationResolver);
        factory.setConcurrency(concurrencyLimits);
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
