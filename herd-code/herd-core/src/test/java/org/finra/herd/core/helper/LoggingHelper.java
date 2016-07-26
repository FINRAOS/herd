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
import java.util.List;

import org.finra.herd.core.Command;

/**
 * A logging helper that provides methods to manipulate logging infrastructure beyond what is provided in SLF4J.
 * The majority of code in the standard code base depends on the SLF4J abstraction layer, but test cases require
 * extra functionality that goes beyond what SLF4J provides. As such, in all of these cases, test classes should call this class
 * when specific functionality is required. That way, if we migrate to a different logging framework, a different implementation
 * could be created and bolted in rather than modifying the all the specific test classes.
 */
public interface LoggingHelper
{
    /**
     * Gets the log level for the specified logger.
     *
     * @param clazz the class for the logger.
     */
    public LogLevel getLogLevel(Class clazz);

    /**
     * Gets the log level for the specified logger.
     *
     * @param loggerName the logger name to get the level for.
     */
    public LogLevel getLogLevel(String loggerName);

    /**
     * Sets the log level.
     *
     * @param clazz the class for the logger.
     * @param logLevel the log level to set.
     */
    public void setLogLevel(Class clazz, LogLevel logLevel);

    /**
     * Sets the log level.
     *
     * @param loggerName the logger name (e.g. Myclass.class.getName()).
     * @param logLevel the log level to set.
     */
    public void setLogLevel(String loggerName, LogLevel logLevel);

    /**
     * Shuts down the logging framework.
     */
    public void shutdownLogging();

    /**
     * Executes a command without logging. The logging will be temporarily turned off during the execution of the command and then restored once the
     * command has
     * finished executing.
     *
     * @param loggingClass the logging class to turn off. If null is specified, the command will be executed with no logging changes
     * @param command the command to execute
     *
     * @throws Exception if any errors were encountered.
     */
    public void executeWithoutLogging(Class<?> loggingClass, Command command) throws Exception;

    /**
     * Executes a command without logging. The logging will be temporarily turned off during the execution of the command and then restored once the
     * command has
     * finished executing.
     *
     * @param loggingClasses the list of logging classes to turn off
     * @param command the command to execute
     *
     * @throws Exception if any errors were encountered
     */
    public void executeWithoutLogging(List<Class<?>> loggingClasses, Command command) throws Exception;

    /**
     * Initializes the logging context.
     *
     * @param contextName the context name.
     * @param configLocation the configuration location (i.e. URL).
     */
    public void initLogging(String contextName, String configLocation);

    /**
     * Adds a logging writer appender.
     *
     * @param appenderName the appender name.
     *
     * @return the string writer associated with the writer appender.
     */
    public StringWriter addLoggingWriterAppender(String appenderName);

    /**
     * Removes a logging appender.
     *
     * @param appenderName the appender name.
     */
    public void removeLoggingAppender(String appenderName);
}
