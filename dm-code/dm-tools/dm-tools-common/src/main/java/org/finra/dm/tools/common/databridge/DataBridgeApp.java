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
package org.finra.dm.tools.common.databridge;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import org.finra.dm.core.ApplicationContextHolder;
import org.finra.dm.core.ArgumentParser;
import org.finra.dm.core.config.CoreSpringModuleConfig;
import org.finra.dm.model.api.xml.BuildInformation;
import org.finra.dm.service.config.ServiceBasicAopSpringModuleConfig;
import org.finra.dm.tools.common.ToolsCommonConstants;
import org.finra.dm.tools.common.config.DataBridgeEnvSpringModuleConfig;
import org.finra.dm.tools.common.config.DataBridgeSpringModuleConfig;

/**
 * A base class for the uploader and downloader applications. This class is abstract since it is not an actual application that has a main method, but rather
 * provides common base functionality that should be used by an extending class.
 */
public abstract class DataBridgeApp
{
    public static final String BUILD_INFO_STRING_FORMAT = "buildDate: %s\nbuildNumber: %s\nbuildOS: %s\nbuildUser: %s";

    private static final Logger LOGGER = Logger.getLogger(DataBridgeApp.class);

    // The common command line options.
    protected Option s3AccessKeyOpt;
    protected Option s3SecretKeyOpt;
    protected Option s3EndpointOpt;
    protected Option localPathOpt;
    protected Option manifestPathOpt;
    protected Option dmRegServerHostOpt;
    protected Option dmRegServerPortOpt;
    protected Option sslOpt;
    protected Option usernameOpt;
    protected Option passwordOpt;
    protected Option helpOpt;
    protected Option versionOpt;
    protected Option httpProxyHostOpt;
    protected Option httpProxyPortOpt;
    protected Option maxThreadsOpt;

    // Boolean values for command line options that are of type "Boolean".
    protected Boolean useSsl;

    // Integer values for command line options that are of type "Integer".
    protected Integer dmRegServerPort;
    protected Integer httpProxyPort;
    protected Integer maxThreads;

    /**
     * The list of possible return values for the application.
     */
    public enum ReturnValue
    {
        SUCCESS(0),
        FAILURE(1);

        private int returnCode;

        private ReturnValue(int returnCode)
        {
            this.returnCode = returnCode;
        }

        public int getReturnCode()
        {
            return returnCode;
        }
    }

    /**
     * Gets the application argument parser.
     *
     * @return the argument parser.
     */
    public abstract ArgumentParser getArgumentParser();

    /**
     * Runs the application. This is the main entry into the application class itself and is typically called from a main method.
     *
     * @param args the command line arguments.
     *
     * @return the application return value.
     * @throws Exception if any problems were encountered.
     */
    public abstract ReturnValue go(String[] args) throws Exception;

    /**
     * Parses the command line arguments using the specified argument parser. Common data bridge options will be initialized and added to the argument parser in
     * this method, but any application specific arguments should be added to the argument parser prior to calling this method. Ensure the argParser was set in
     * this class prior to calling this method.
     *
     * @param args the command line arguments.
     * @param applicationContext the Spring application context.
     *
     * @return the return value if the application should exit or null if the application can continue.
     */
    // Using System.out to inform user of usage or version information is okay.
    @SuppressWarnings("PMD.SystemPrintln")
    @SuppressFBWarnings(value = "VA_FORMAT_STRING_USES_NEWLINE", justification = "We will use the standard carriage return character.")
    protected ReturnValue parseCommandLineArguments(String[] args, ApplicationContext applicationContext)
    {
        // Get the application argument parser.
        ArgumentParser argParser = getArgumentParser();

        try
        {
            s3AccessKeyOpt = argParser.addArgument("a", "s3AccessKey", true, "S3 access key.", false);
            s3SecretKeyOpt = argParser.addArgument("p", "s3SecretKey", true, "S3 secret key.", false);
            s3EndpointOpt = argParser.addArgument("e", "s3Endpoint", true, "S3 endpoint.", false);
            localPathOpt = argParser.addArgument("l", "localPath", true, "The path to files on your local file system.", true);
            manifestPathOpt = argParser.addArgument("m", "manifestPath", true, "Local path to the manifest file.", true);
            dmRegServerHostOpt = argParser.addArgument("H", "dmRegServerHost", true, "Data Management Registration Service hostname.", true);
            dmRegServerPortOpt = argParser.addArgument("P", "dmRegServerPort", true, "Data Management Registration Service port.", true);
            sslOpt = argParser.addArgument("s", "ssl", true, "Enable or disable SSL (HTTPS).", false);
            usernameOpt = argParser.addArgument("u", "username", true, "The username for HTTPS client authentication.", false);
            passwordOpt = argParser.addArgument("w", "password", true, "The password used for HTTPS client authentication.", false);
            helpOpt = argParser.addArgument("h", "help", false, "Display usage information and exit.", false);
            versionOpt = argParser.addArgument("v", "version", false, "Display version information and exit.", false);
            httpProxyHostOpt = argParser.addArgument("n", "httpProxyHost", true, "HTTP proxy host.", false);
            httpProxyPortOpt = argParser.addArgument("o", "httpProxyPort", true, "HTTP proxy port.", false);
            maxThreadsOpt = argParser.addArgument("t", "maxThreads", true, "Maximum number of threads.", false);

            // Parse command line arguments without failing on any missing required arguments by passing "false" as the second argument.
            argParser.parseArguments(args, false);

            // If help option was specified, then display usage information and return success.
            if (argParser.getBooleanValue(helpOpt))
            {
                System.out.println(argParser.getUsageInformation());
                return ReturnValue.SUCCESS;
            }

            // If version option was specified, then display version information and return success.
            if (argParser.getBooleanValue(versionOpt))
            {
                BuildInformation buildInformation = applicationContext.getBean(BuildInformation.class);
                System.out.println(String
                    .format(BUILD_INFO_STRING_FORMAT, buildInformation.getBuildDate(), buildInformation.getBuildNumber(), buildInformation.getBuildOs(),
                        buildInformation.getBuildUser()));
                return ReturnValue.SUCCESS;
            }

            // Parse command line arguments for the second time, enforcing the required arguments by passing "true" as the second argument.
            argParser.parseArguments(args, true);

            // Extract a boolean option value passing "false" as a default value.
            useSsl = argParser.getStringValueAsBoolean(sslOpt, false);

            // Username and password are required when useSsl is enabled.
            if (useSsl && (StringUtils.isBlank(argParser.getStringValue(usernameOpt)) || StringUtils.isBlank(argParser.getStringValue(passwordOpt))))
            {
                throw new ParseException("Username and password are required when SSL is enabled.");
            }

            // Ensure that both the S3 secret and access keys were specified or both not specified.
            if (StringUtils.isNotBlank(argParser.getStringValue(s3SecretKeyOpt)) && StringUtils.isBlank(argParser.getStringValue(s3AccessKeyOpt)))
            {
                throw new ParseException("S3 access key must be specified when S3 secret key is present.");
            }
            if (StringUtils.isNotBlank(argParser.getStringValue(s3AccessKeyOpt)) && StringUtils.isBlank(argParser.getStringValue(s3SecretKeyOpt)))
            {
                throw new ParseException("S3 secret key must be specified when S3 access key is present.");
            }

            // Extract all Integer option values here to catch any NumberFormatException exceptions.
            dmRegServerPort = argParser.getIntegerValue(dmRegServerPortOpt);
            httpProxyPort = argParser.getIntegerValue(httpProxyPortOpt);
            maxThreads = argParser.getIntegerValue(maxThreadsOpt, ToolsCommonConstants.DEFAULT_THREADS);
        }
        catch (ParseException ex)
        {
            // Log a friendly error and return a failure which will cause the application to exit.
            LOGGER.error("Error parsing command line arguments: " + ex.getMessage() + "\n" + argParser.getUsageInformation());
            return ReturnValue.FAILURE;
        }

        // The command line arguments were all parsed successfully so return null to continue processing.
        return null;
    }

    /**
     * Creates and returns the Spring application context.
     *
     * @return the application context
     */
    protected ApplicationContext createApplicationContext()
    {
        // Create the Spring application context and register the JavaConfig classes we need.
        // We will use core (in case it's needed), the service aspect that times the duration of the service method calls, and our specific beans defined in
        // the data bridge configuration. We're not including full service and DAO configurations because they come with database/data source dependencies
        // that we don't need and don't want (i.e. we don't want the database to be running as a pre-requisite for running the uploader).
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        ApplicationContextHolder.setApplicationContext(applicationContext);
        applicationContext.register(CoreSpringModuleConfig.class, ServiceBasicAopSpringModuleConfig.class, DataBridgeSpringModuleConfig.class,
            DataBridgeEnvSpringModuleConfig.class);
        applicationContext.refresh();
        return applicationContext;
    }
}
