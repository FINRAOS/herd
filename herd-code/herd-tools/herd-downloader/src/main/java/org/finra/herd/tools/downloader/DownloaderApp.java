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
package org.finra.herd.tools.downloader;

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
 * The "main" downloader data bridge application command line tool.
 */
public class DownloaderApp extends DataBridgeApp
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloaderApp.class);

    // An argument parser for the application.
    private ArgumentParser argParser;

    /**
     * Constructs a new UploaderApp instance.
     */
    public DownloaderApp()
    {
        argParser = new ArgumentParser("herd-downloader-app");
    }

    /**
     * Parses the command line arguments and calls the controller to process the download.
     *
     * @param args the command line arguments passed to the program.
     *
     * @return the return value of the application.
     * @throws Exception if there are problems performing the download.
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
        S3FileTransferRequestParamsDto params = S3FileTransferRequestParamsDto.builder().withLocalPath(argParser.getStringValue(localPathOpt))
            .withAwsAccessKeyId(argParser.getStringValue(s3AccessKeyOpt)).withAwsSecretKey(argParser.getStringValue(s3SecretKeyOpt))
            .withS3Endpoint(argParser.getStringValue(s3EndpointOpt)).withMaxThreads(maxThreads).withHttpProxyHost(argParser.getStringValue(httpProxyHostOpt))
            .withHttpProxyPort(httpProxyPort).withSocketTimeout(argParser.getIntegerValue(socketTimeoutOpt)).build();

        // Call the controller with the user specified parameters to perform the download.
        DownloaderController controller = applicationContext.getBean(DownloaderController.class);
        RegServerAccessParamsDto regServerAccessParamsDto =
            RegServerAccessParamsDto.builder().withRegServerHost(regServerHost).withRegServerPort(regServerPort).withUseSsl(useSsl)
                .withUsername(argParser.getStringValue(usernameOpt)).withPassword(argParser.getStringValue(passwordOpt))
                .withTrustSelfSignedCertificate(trustSelfSignedCertificate).withDisableHostnameVerification(disableHostnameVerification).build();
        controller.performDownload(regServerAccessParamsDto, argParser.getFileValue(manifestPathOpt), params);

        // No exceptions were returned so return success.
        return ReturnValue.SUCCESS;
    }

    @Override
    public ArgumentParser getArgumentParser()
    {
        return argParser;
    }

    /**
     * The main method of the Downloader Application.
     *
     * @param args the command line arguments passed to the program.
     */
    @SuppressWarnings("PMD.DoNotCallSystemExit") // Using System.exit is allowed for an actual application to exit.
    public static void main(String[] args)
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

            DownloaderApp downloaderApp = new DownloaderApp();
            returnValue = downloaderApp.go(args);
        }
        catch (Exception e)
        {
            LOGGER.error("Error running herd downloader. {}", e.toString(), e);
            returnValue = ReturnValue.FAILURE;
        }

        // Exit with the return code.
        System.exit(returnValue.getReturnCode());
    }
}
