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
package org.finra.herd.tools.uploader;

import java.io.FileNotFoundException;

import org.apache.commons.cli.Option;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import org.finra.herd.core.ArgumentParser;
import org.finra.herd.model.dto.RegServerAccessParamsDto;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;
import org.finra.herd.tools.common.ToolsCommonConstants;
import org.finra.herd.tools.common.databridge.DataBridgeApp;

/**
 * The "main" uploader data bridge application command line tool.
 */
public class UploaderApp extends DataBridgeApp
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UploaderApp.class);

    // The uploader specific command line options.
    private Option maxRetryAttemptsOpt;

    private Option retryDelaySecsOpt;

    private Option createNewVersionOpt;

    private Option rrsOpt;

    private Option forceOpt;

    // Integer values for command line options that are of type "Integer".
    private Integer maxRetryAttempts;

    private Integer retryDelaySecs;

    // An argument parser for the application.
    private ArgumentParser argParser;

    private static final Integer MAX_RETRY_ATTEMPTS_DEFAULT = 5;    // Default number of business object data registration retry attempts.

    private static final Integer MAX_RETRY_ATTEMPTS_MIN = 0;        // Minimum number of business object data registration retry attempts.

    private static final Integer MAX_RETRY_ATTEMPTS_MAX = 10;       // Maximum number of business object data registration retry attempts.

    private static final Integer RETRY_DELAY_SECS_DEFAULT = 120;    // Default delay in seconds between the business object data registration retry attempts.

    private static final Integer RETRY_DELAY_SECS_MIN = 0;          // Minimum delay in seconds between the business object data registration retry attempts.

    private static final Integer RETRY_DELAY_SECS_MAX = 900;        // Maximum delay in seconds between the business object data registration retry attempts.

    /**
     * Constructs a new UploaderApp instance.
     */
    public UploaderApp()
    {
        argParser = new ArgumentParser("herd-uploader-app");

        // Create command line options specific to the uploader. Other common options will be handled by the base class.
        createNewVersionOpt = argParser
            .addArgument("V", "createNewVersion", false, "If not set, only initial version of the business object data is allowed to be created.", false);
        rrsOpt = argParser.addArgument("r", "rrs", false, "If set, the data will be saved in Reduced Redundancy Storage.", false);
        maxRetryAttemptsOpt = argParser.addArgument("R", "maxRetryAttempts", true,
            "The maximum number of the business object data registration retry attempts that uploader would perform before rolling back the upload.", false);
        retryDelaySecsOpt =
            argParser.addArgument("D", "retryDelaySecs", true, "The delay in seconds between the business object data registration retry attempts.", false);
        forceOpt = argParser.addArgument("f", "force", false,
            "If set, allows upload to proceed when the latest version of the business object data has UPLOADING status by invalidating that version.", false);
    }

    /**
     * Parses the command line arguments using the specified argument parser.
     *
     * @param args the command line arguments.
     * @param applicationContext the Spring application context.
     *
     * @return the return value if the application should exit or null if the application can continue.
     */
    @Override
    protected ReturnValue parseCommandLineArguments(String[] args, ApplicationContext applicationContext)
    {
        ReturnValue returnValue = super.parseCommandLineArguments(args, applicationContext);

        // Stop the processing if return value is not null.
        if (returnValue != null)
        {
            return returnValue;
        }

        try
        {
            // Extract uploader specific Integer option values here to catch any NumberFormatException exceptions.
            maxRetryAttempts = argParser.getIntegerValue(maxRetryAttemptsOpt, MAX_RETRY_ATTEMPTS_DEFAULT, MAX_RETRY_ATTEMPTS_MIN, MAX_RETRY_ATTEMPTS_MAX);
            retryDelaySecs = argParser.getIntegerValue(retryDelaySecsOpt, RETRY_DELAY_SECS_DEFAULT, RETRY_DELAY_SECS_MIN, RETRY_DELAY_SECS_MAX);
        }
        catch (Exception ex)
        {
            // Log a friendly error and return a failure which will cause the application to exit.
            LOGGER.error("Error parsing command line arguments: " + ex.getMessage() + "\n" + argParser.getUsageInformation());
            return ReturnValue.FAILURE;
        }

        // The command line arguments were all parsed successfully so return null to continue processing.
        return null;
    }

    /**
     * Parses the command line arguments and calls the controller to process the upload.
     *
     * @param args the command line arguments passed to the program.
     *
     * @return the return value of the application.
     * @throws Exception if there are problems performing the upload.
     */
    @Override
    public ReturnValue go(String[] args) throws Exception
    {
        // Create the Spring application context.
        ApplicationContext applicationContext = createApplicationContext();

        // Parse the command line arguments and return a return value if we shouldn't continue processing (e.g. we displayed usage information, etc.).
        ReturnValue returnValue = parseCommandLineArguments(args, applicationContext);
        if (returnValue != null)
        {
            return returnValue;
        }

        // Create an instance of S3 file transfer request parameters DTO.
        S3FileTransferRequestParamsDto params =
            S3FileTransferRequestParamsDto.builder().withLocalPath(argParser.getStringValue(localPathOpt)).withUseRrs(argParser.getBooleanValue(rrsOpt))
                .withAwsAccessKeyId(argParser.getStringValue(s3AccessKeyOpt)).withAwsSecretKey(argParser.getStringValue(s3SecretKeyOpt))
                .withS3Endpoint(argParser.getStringValue(s3EndpointOpt)).withMaxThreads(maxThreads)
                .withHttpProxyHost(argParser.getStringValue(httpProxyHostOpt)).withHttpProxyPort(httpProxyPort)
                .withSocketTimeout(argParser.getIntegerValue(socketTimeoutOpt)).build();

        // Call the controller with the user specified parameters to perform the upload.
        UploaderController controller = applicationContext.getBean(UploaderController.class);
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(regServerHost).withRegServerPort(regServerPort).withUseSsl(useSsl)
                .withUsername(argParser.getStringValue(usernameOpt)).withPassword(argParser.getStringValue(passwordOpt))
                .withTrustSelfSignedCertificate(trustSelfSignedCertificate).withDisableHostnameVerification(disableHostnameVerification).build();
        controller.performUpload(regServerAccessParamsDto, argParser.getFileValue(manifestPathOpt), params, argParser.getBooleanValue(createNewVersionOpt),
            argParser.getBooleanValue(forceOpt), maxRetryAttempts, retryDelaySecs);

        // No exceptions were returned so return success.
        return ReturnValue.SUCCESS;
    }

    @Override
    public ArgumentParser getArgumentParser()
    {
        return argParser;
    }

    /**
     * The main method of the Uploader Application.
     *
     * @param args the command line arguments passed to the program.
     *
     * @throws FileNotFoundException if the logging file couldn't be found.
     */
    @SuppressWarnings("PMD.DoNotCallSystemExit") // Using System.exit is allowed for an actual application to exit.
    public static void main(String[] args) throws FileNotFoundException
    {
        ReturnValue returnValue;
        try
        {
            // Initialize Log4J with the resource. The configuration itself can use "monitorInterval" to have it refresh if it came from a file.
            LoggerContext loggerContext = Configurator.initialize(null, ToolsCommonConstants.LOG4J_CONFIG_LOCATION);

            // For some initialization errors, a null context will be returned.
            if (loggerContext == null)
            {
                // We shouldn't get here since we already checked if the location existed previously.
                throw new IllegalArgumentException("Invalid configuration found at resource location: \"" + ToolsCommonConstants.LOG4J_CONFIG_LOCATION + "\".");
            }

            UploaderApp uploaderApp = new UploaderApp();
            returnValue = uploaderApp.go(args);
        }
        catch (Exception e)
        {
            LOGGER.error("Error running herd uploader. {}", e.toString(), e);
            returnValue = ReturnValue.FAILURE;
        }

        // Exit with the return code.
        System.exit(returnValue.getReturnCode());
    }
}
