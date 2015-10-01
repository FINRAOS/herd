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
package org.finra.dm.ui.config;

import java.util.ArrayList;
import java.util.List;

import org.activiti.engine.IdentityService;
import org.activiti.engine.ProcessEngine;
import org.activiti.explorer.ComponentFactories;
import org.activiti.explorer.ExplorerApp;
import org.activiti.explorer.I18nManager;
import org.activiti.explorer.NotificationManager;
import org.activiti.explorer.ViewManagerFactoryBean;
import org.activiti.explorer.demo.DemoDataGenerator;
import org.activiti.explorer.navigation.NavigationFragmentChangeListener;
import org.activiti.explorer.navigation.NavigatorManager;
import org.activiti.explorer.ui.MainWindow;
import org.activiti.explorer.ui.content.AttachmentRendererManager;
import org.activiti.explorer.ui.form.BooleanFormPropertyRenderer;
import org.activiti.explorer.ui.form.DateFormPropertyRenderer;
import org.activiti.explorer.ui.form.DoubleFormPropertyRenderer;
import org.activiti.explorer.ui.form.EnumFormPropertyRenderer;
import org.activiti.explorer.ui.form.FormPropertyRenderer;
import org.activiti.explorer.ui.form.FormPropertyRendererManager;
import org.activiti.explorer.ui.form.LongFormPropertyRenderer;
import org.activiti.explorer.ui.form.MonthFormPropertyRenderer;
import org.activiti.explorer.ui.form.ProcessDefinitionFormPropertyRenderer;
import org.activiti.explorer.ui.form.StringFormPropertyRenderer;
import org.activiti.explorer.ui.form.UserFormPropertyRenderer;
import org.activiti.explorer.ui.login.DefaultLoginHandler;
import org.activiti.explorer.ui.management.deployment.DeploymentFilterFactory;
import org.activiti.explorer.ui.process.ProcessDefinitionFilterFactory;
import org.activiti.explorer.ui.variable.VariableRendererManager;
import org.activiti.workflow.simple.converter.WorkflowDefinitionConversionFactory;
import org.activiti.workflow.simple.converter.json.SimpleWorkflowJsonConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * UI Spring module configuration.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.dm.ui",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.dm\\.ui\\.config\\..*"))
@EnableWebMvc
public class UiSpringModuleConfig
{
    @Autowired
    private ProcessEngine activitiProcessEngine;

    @Autowired
    private IdentityService activitiIdentityService;

    // ********** Activiti UI Beans **********

    /**
     * Gets a demo data generator.
     *
     * @return the demo data generator.
     */
    @Bean(initMethod = "init")
    public DemoDataGenerator demoDataGenerator()
    {
        DemoDataGenerator generator = new DemoDataGenerator();

        generator.setProcessEngine(activitiProcessEngine);
        generator.setCreateDemoUsersAndGroups(false);
        generator.setCreateDemoModels(false);
        generator.setCreateDemoProcessDefinitions(false);
        generator.setGenerateReportData(false);

        return generator;
    }

    /**
     * Gets an Activiti login handler.
     *
     * @return the Activiti login handler.
     */
    @Bean
    public DefaultLoginHandler activitiLoginHandler()
    {
        DefaultLoginHandler loginHandler = new DefaultLoginHandler();
        loginHandler.setIdentityService(activitiIdentityService);
        return loginHandler;
    }

    @Bean
    public NavigatorManager navigatorManager()
    {
        return new NavigatorManager();
    }

    @Bean
    public AttachmentRendererManager attachmentRendererManager()
    {
        return new AttachmentRendererManager();
    }

    /**
     * Gets a form property renderer manager.
     *
     * @return the form property renderer manager.
     */
    @Bean
    @Lazy(true)
    public FormPropertyRendererManager formPropertyRendererManager()
    {
        FormPropertyRendererManager renderManager = new FormPropertyRendererManager();
        renderManager.setNoTypePropertyRenderer(new StringFormPropertyRenderer());

        List<FormPropertyRenderer> customFormTypeList = new ArrayList<>();
        customFormTypeList.add(new StringFormPropertyRenderer());
        customFormTypeList.add(new EnumFormPropertyRenderer());
        customFormTypeList.add(new LongFormPropertyRenderer());
        customFormTypeList.add(new DoubleFormPropertyRenderer());
        customFormTypeList.add(new DateFormPropertyRenderer());
        customFormTypeList.add(new UserFormPropertyRenderer());
        customFormTypeList.add(new BooleanFormPropertyRenderer());
        customFormTypeList.add(new ProcessDefinitionFormPropertyRenderer());
        customFormTypeList.add(new MonthFormPropertyRenderer());

        renderManager.setPropertyRenderers(customFormTypeList);

        return renderManager;
    }

    @Bean
    public VariableRendererManager variableRendererManager()
    {
        return new VariableRendererManager();
    }

    /**
     * Get component factories.
     *
     * @return the component factories.
     */
    @Bean
    public ComponentFactories componentFactories()
    {
        ComponentFactories factories = new ComponentFactories();
        factories.setEnvironment("activiti");
        return factories;
    }

    /**
     * Gets a process definition filter factory.
     *
     * @return the process definition filter factory.
     */
    @Bean
    public ProcessDefinitionFilterFactory processDefinitionFilterFactory()
    {
        ProcessDefinitionFilterFactory factory = new ProcessDefinitionFilterFactory();
        factory.setComponentFactories(componentFactories());
        return factory;
    }

    /**
     * Gets a deployment filter factory.
     *
     * @return the deployment filter factory.
     */
    @Bean
    public DeploymentFilterFactory deploymentFilterFactory()
    {
        DeploymentFilterFactory factory = new DeploymentFilterFactory();
        factory.setComponentFactories(componentFactories());
        return factory;
    }

    /**
     * Gets a navigation fragment change listener.
     *
     * @return the navigation fragment change listener.
     */
    @Bean
    @Scope(value = "session")
    public NavigationFragmentChangeListener navigationFragmentChangeListener()
    {
        NavigationFragmentChangeListener listener = new NavigationFragmentChangeListener();
        listener.setNavigatorManager(navigatorManager());
        return listener;
    }

    /**
     * Gets the main window.
     *
     * @return the main window.
     */
    @Bean
    @Scope(value = "session")
    public MainWindow mainWindow()
    {
        MainWindow mainWindow = new MainWindow();
        mainWindow.setNavigationFragmentChangeListener(navigationFragmentChangeListener());
        mainWindow.setI18nManager(i18nManager());
        return mainWindow;
    }

    /**
     * Gets an explorer application.
     *
     * @return the explorer application.
     * @throws Exception if the view manager couldn't be retrieved.
     */
    @Bean
    @Scope(value = "session")
    public ExplorerApp explorerApp() throws Exception
    {
        ExplorerApp explorerApp = new ExplorerApp();
        explorerApp.setEnvironment("activiti");
        explorerApp.setUseJavascriptDiagram(true);
        explorerApp.setI18nManager(i18nManager());
        explorerApp.setViewManager(viewManager().getObject());
        explorerApp.setNotificationManager(notificationManager());
        explorerApp.setAttachmentRendererManager(attachmentRendererManager());
        explorerApp.setFormPropertyRendererManager(formPropertyRendererManager());
        explorerApp.setVariableRendererManager(variableRendererManager());
        explorerApp.setApplicationMainWindow(mainWindow());
        explorerApp.setComponentFactories(componentFactories());
        explorerApp.setWorkflowDefinitionConversionFactory(workflowDefinitionConversionFactory());
        explorerApp.setLoginHandler(activitiLoginHandler());
        explorerApp.setWorkflowDefinitionConversionFactory(workflowDefinitionConversionFactory());

        return explorerApp;
    }

    @Bean
    public SimpleWorkflowJsonConverter simpleWorkflowJsonConverter()
    {
        return new SimpleWorkflowJsonConverter();
    }

    /**
     * Gets an I18n manager.
     *
     * @return the I18n manager.
     */
    @Bean
    @Scope(value = "session")
    public I18nManager i18nManager()
    {
        I18nManager i18nManager = new I18nManager();
        i18nManager.setMessageSource(messageSource());
        return i18nManager;
    }

    /**
     * Gets a message source.
     *
     * @return the message source.
     */
    @Bean
    @Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
    public ResourceBundleMessageSource messageSource()
    {
        ResourceBundleMessageSource messageSource = new ResourceBundleMessageSource();
        messageSource.setBasenames("messages");
        return messageSource;
    }

    /**
     * Gets a notification manager.
     *
     * @return the notification manager.
     */
    @Bean
    @Scope(value = "session")
    public NotificationManager notificationManager()
    {
        NotificationManager notificationManager = new NotificationManager();
        notificationManager.setMainWindow(mainWindow());
        notificationManager.setI18nManager(i18nManager());
        return notificationManager;
    }

    /**
     * Gets the view manager.
     *
     * @return the view manager.
     */
    @Bean
    @Scope(value = "session")
    public ViewManagerFactoryBean viewManager()
    {
        ViewManagerFactoryBean viewManager = new ViewManagerFactoryBean();
        viewManager.setMainWindow(mainWindow());
        viewManager.setEnvironment("activiti");
        return viewManager;
    }

    @Bean
    public WorkflowDefinitionConversionFactory workflowDefinitionConversionFactory()
    {
        return new WorkflowDefinitionConversionFactory();
    }
}
