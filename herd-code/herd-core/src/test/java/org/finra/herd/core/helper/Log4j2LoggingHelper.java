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
package org.finra.herd.core.helper;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.finra.herd.core.Command;

/**
 * Log4J2 implementation of the logging helper.
 */
@Component
public class Log4j2LoggingHelper implements LoggingHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Log4j2LoggingHelper.class);

    @Override
    public LogLevel getLogLevel(Class clazz)
    {
        return getLogLevel(clazz.getName());
    }

    @Override
    public LogLevel getLogLevel(String loggerName)
    {
        // Get the main logger context.
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);

        // Get the main configuration.
        Configuration configuration = loggerContext.getConfiguration();

        // Get the logging configuration for the specified logger name from the main configuration.
        LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);

        // Return the logging level for the logging configuration.
        return log4jToGenericLogLevel(loggerConfig.getLevel());
    }

    @Override
    public void setLogLevel(Class clazz, LogLevel logLevel)
    {
        setLogLevel(clazz.getName(), logLevel);
    }

    @Override
    public void setLogLevel(String loggerName, LogLevel logLevel)
    {
        Configurator.setLevel(loggerName, genericLogLevelToLog4j(logLevel));
    }

    @Override
    public void shutdownLogging()
    {
        LogManager.shutdown();
    }

    @Override
    public void executeWithoutLogging(Class<?> loggingClass, Command command) throws Exception
    {
        List<Class<?>> loggingClasses = new ArrayList<>();
        loggingClasses.add(loggingClass);
        executeWithoutLogging(loggingClasses, command);
    }

    @Override
    public void executeWithoutLogging(List<Class<?>> loggingClasses, Command command) throws Exception
    {
        // Get the logger context and it's associated configuration.
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = loggerContext.getConfiguration();

        // Temporarily turn off logging.
        Map<LoggerConfig, Level> loggerToOriginalLevel = new HashMap<>();
        for (Class<?> loggingClass : loggingClasses)
        {
            LoggerConfig loggerConfig = loggingClass == null ? null : configuration.getLoggerConfig(loggingClass.getName());
            Level originalLevel = loggerConfig == null ? null : loggerConfig.getLevel();
            if (loggerConfig != null)
            {
                loggerConfig.setLevel(Level.OFF);
                loggerToOriginalLevel.put(loggerConfig, originalLevel);
            }
        }

        // This causes all Loggers to re-fetch information from their LoggerConfig.
        loggerContext.updateLoggers();

        try
        {
            // Execute the command.
            command.execute();
        }
        finally
        {
            for (Map.Entry<LoggerConfig, Level> entry : loggerToOriginalLevel.entrySet())
            {
                LoggerConfig loggerConfig = entry.getKey();
                Level originalLevel = entry.getValue();

                // Turn the original logging back on.
                loggerConfig.setLevel(originalLevel);
            }
        }
    }

    @Override
    public void initLogging(String contextName, String configLocation)
    {
        // Initialize via the configurator.
        Configurator.initialize(contextName, null, configLocation);
    }

    @Override
    public StringWriter addLoggingWriterAppender(String appenderName)
    {
        // Get the configuration
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = loggerContext.getConfiguration();

        // Create a string writer as part of the appender so logging will be written to it.
        StringWriter stringWriter = new StringWriter();

        // Create and start the appender with the string writer.
        Appender appender = WriterAppender.createAppender(null, null, stringWriter, appenderName, false, true);
        appender.start();

        // Add the appender to the root logger.
        configuration.getRootLogger().addAppender(appender, null, null);

        // Return the string writer.
        return stringWriter;
    }

    @Override
    public void removeLoggingAppender(String appenderName)
    {
        // Get the configuration and remove the appender from it.
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        final Configuration configuration = loggerContext.getConfiguration();
        configuration.getRootLogger().removeAppender(appenderName);
    }

    /**
     * Converts a Log4J specific log level to a generic log level.
     *
     * @param level the Log4J log level.
     *
     * @return the generic log level.
     */
    private LogLevel log4jToGenericLogLevel(Level level)
    {
        LogLevel logLevel = LogLevel.ALL;
        if (level.equals(Level.OFF))
        {
            logLevel = LogLevel.OFF;
        }
        else if (level.equals(Level.FATAL))
        {
            logLevel = LogLevel.FATAL;
        }
        else if (level.equals(Level.ERROR))
        {
            logLevel = LogLevel.ERROR;
        }
        else if (level.equals(Level.WARN))
        {
            logLevel = LogLevel.WARN;
        }
        else if (level.equals(Level.INFO))
        {
            logLevel = LogLevel.INFO;
        }
        else if (level.equals(Level.DEBUG))
        {
            logLevel = LogLevel.DEBUG;
        }
        else if (level.equals(Level.TRACE))
        {
            logLevel = LogLevel.TRACE;
        }
        else if (level.equals(Level.ALL))
        {
            logLevel = LogLevel.ALL;
        }
        else
        {
            LOGGER.warn("Unsupported log level encountered: " + level.toString() + ". Using ALL.");
        }
        return logLevel;
    }

    /**
     * Converts a generic log level to a Log4J specific log level.
     *
     * @param logLevel the generic log level.
     *
     * @return the Log4J specific log level.
     */
    private Level genericLogLevelToLog4j(LogLevel logLevel)
    {
        Level level = Level.ALL;
        if (logLevel.equals(LogLevel.OFF))
        {
            level = Level.OFF;
        }
        else if (logLevel.equals(LogLevel.FATAL))
        {
            level = Level.FATAL;
        }
        else if (logLevel.equals(LogLevel.ERROR))
        {
            level = Level.ERROR;
        }
        else if (logLevel.equals(LogLevel.WARN))
        {
            level = Level.WARN;
        }
        else if (logLevel.equals(LogLevel.INFO))
        {
            level = Level.INFO;
        }
        else if (logLevel.equals(LogLevel.DEBUG))
        {
            level = Level.DEBUG;
        }
        else if (logLevel.equals(LogLevel.TRACE))
        {
            level = Level.TRACE;
        }
        else if (logLevel.equals(LogLevel.ALL))
        {
            level = Level.ALL;
        }
        else
        {
            LOGGER.warn("Unsupported log level encountered: " + logLevel.toString() + ". Using ALL.");
        }
        return level;
    }
}
