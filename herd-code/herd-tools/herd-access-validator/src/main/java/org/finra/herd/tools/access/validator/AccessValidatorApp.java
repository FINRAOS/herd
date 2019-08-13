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
package org.finra.herd.tools.access.validator;

import java.io.File;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.ArgumentParser;
import org.finra.herd.core.config.CoreSpringModuleConfig;
import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.sdk.invoker.ApiException;
import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.config.DataBridgeAopSpringModuleConfig;
import org.finra.herd.tools.common.config.DataBridgeEnvSpringModuleConfig;
import org.finra.herd.tools.common.config.DataBridgeSpringModuleConfig;

/**
 * A main class for the herd access validator application.
 */
public class AccessValidatorApp
{
    static final String APPLICATION_NAME = "herd-access-validator-app";

    private static final String DEFAULT_PROPERTIES_FILE_PATH = ".properties";

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessValidatorApp.class);

    private ArgumentParser argParser;

    private Option propertiesFilePathOpt;

    private Option messageOpt;

    AccessValidatorApp()
    {
        argParser = new ArgumentParser(APPLICATION_NAME);
    }

    /**
     * The main method of the application.
     *
     * @param args the command line arguments passed to the program
     */
    @SuppressWarnings("PMD.DoNotCallSystemExit") // Using System.exit is allowed for an actual application to exit.
    public static void main(String[] args)
    {
        ToolsCommonConstants.ReturnValue returnValue;
        try
        {
            // Initialize Log4J with the resource. The configuration itself can use "monitorInterval" to have it refresh if it came from a file.
            LoggerContext loggerContext = Configurator.initialize(null, ToolsCommonConstants.LOG4J_CONFIG_LOCATION);

            // For some initialization errors, a null context will be returned.
            if (loggerContext == null)
            {
                // We shouldn't get here since we already checked if the location existed previously.
                throw new IllegalArgumentException(
                    String.format("Invalid configuration found at resource location: \"%s\".", ToolsCommonConstants.LOG4J_CONFIG_LOCATION));
            }

            AccessValidatorApp exporterApp = new AccessValidatorApp();
            returnValue = exporterApp.go(args);
        }
        catch (ApiException apiException)
        {
            LOGGER.error("Error running herd access validator. {} statusCode={}", apiException.toString(), apiException.getCode());
            returnValue = ToolsCommonConstants.ReturnValue.FAILURE;
        }
        catch (Exception e)
        {
            LOGGER.error("Error running herd access validator. {}", e.toString());
            returnValue = ToolsCommonConstants.ReturnValue.FAILURE;
        }

        // Exit with the return code.
        System.exit(returnValue.getReturnCode());
    }

    /**
     * Creates and returns the Spring application context.
     *
     * @return the application context
     */
    ApplicationContext createApplicationContext()
    {
        // Create the Spring application context and register the JavaConfig classes we need.
        // We will use core (in case it's needed), the service aspect that times the duration of the service method calls, and our specific beans defined in
        // the data bridge configuration. We're not including full service and DAO configurations because they come with database/data source dependencies
        // that we don't need and don't want (i.e. we don't want the database to be running as a pre-requisite for running the application).
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        ApplicationContextHolder.setApplicationContext(applicationContext);
        applicationContext.register(CoreSpringModuleConfig.class, DataBridgeSpringModuleConfig.class, DataBridgeAopSpringModuleConfig.class,
            DataBridgeEnvSpringModuleConfig.class);
        applicationContext.refresh();
        return applicationContext;
    }

    /**
     * Runs the application by parsing the command line arguments and calling the controller to export business object data that passed its relative retention
     * expiration. This is the main entry into the application class itself and is typically called from a main method.
     *
     * @param args the command line arguments passed to the program
     *
     * @return the return value of the application
     * @throws Exception if any problems were encountered
     */
    ToolsCommonConstants.ReturnValue go(String[] args) throws Exception
    {
        // Create the Spring application context.
        ApplicationContext applicationContext = createApplicationContext();

        // Parse the command line arguments and return a return value if we shouldn't continue processing (e.g. we displayed usage information, etc.).
        ToolsCommonConstants.ReturnValue returnValue = parseCommandLineArguments(args, applicationContext);
        if (returnValue != null)
        {
            return returnValue;
        }

        // Call the controller with the user specified parameters to perform access validation.
        AccessValidatorController controller = applicationContext.getBean(AccessValidatorController.class);
        File propertiesFile = argParser.getFileValue(propertiesFilePathOpt, new File(DEFAULT_PROPERTIES_FILE_PATH));
        Boolean messageFlag = argParser.getBooleanValue(messageOpt);
        controller.validateAccess(propertiesFile, messageFlag);

        // No exceptions were returned so return success.
        return ToolsCommonConstants.ReturnValue.SUCCESS;
    }

    /**
     * Parses the command line arguments using the argument parser. The command line options will be initialized and added to the argument parser in this
     * method. Ensure the argParser was set in this class prior to calling this method.
     *
     * @param args the command line arguments
     * @param applicationContext the Spring application context
     *
     * @return the return value if the application should exit or null if the application can continue.
     */
    // Using System.out to inform user of usage or version information is okay.
    @SuppressWarnings("PMD.SystemPrintln")
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
    ToolsCommonConstants.ReturnValue parseCommandLineArguments(String[] args, ApplicationContext applicationContext)
    {
        try
        {
            propertiesFilePathOpt = argParser.addArgument("p", "properties", true, "Path to the properties file. Defaults to '.properties'.", false);
            Option helpOpt = argParser.addArgument("h", "help", false, "Display usage information and exit.", false);
            Option versionOpt = argParser.addArgument("v", "version", false, "Display version information and exit.", false);
            messageOpt = argParser.addArgument("m", "message", false, "Use an AWS SQS message", false);


            // Parse command line arguments without failing on any missing required arguments by passing "false" as the second argument.
            argParser.parseArguments(args, false);

            // If help option was specified, then display usage information and return success.
            if (argParser.getBooleanValue(helpOpt))
            {
                System.out.println(argParser.getUsageInformation());
                return ToolsCommonConstants.ReturnValue.SUCCESS;
            }

            // If version option was specified, then display version information and return success.
            if (argParser.getBooleanValue(versionOpt))
            {
                BuildInformation buildInformation = applicationContext.getBean(BuildInformation.class);
                System.out.println(String
                    .format(ToolsCommonConstants.BUILD_INFO_STRING_FORMAT, buildInformation.getBuildDate(), buildInformation.getBuildNumber(),
                        buildInformation.getBuildOs(), buildInformation.getBuildUser()));
                return ToolsCommonConstants.ReturnValue.SUCCESS;
            }

            // Parse command line arguments for the second time, enforcing the required arguments by passing "true" as the second argument.
            argParser.parseArguments(args, true);
        }
        catch (ParseException e)
        {
            // Log a friendly error and return a failure which will cause the application to exit.
            LOGGER.error("Error parsing command line arguments: {}%n{}", e.getMessage(), argParser.getUsageInformation());
            return ToolsCommonConstants.ReturnValue.FAILURE;
        }

        // The command line arguments were all parsed successfully so return null to continue processing.
        return null;
    }
}
