package org.finra.herd.core.helper;

/**
 * An enum that defines all the log levels. This is provided so the test code doesn't have to depend on a specific logging implementation.
 */
public enum LogLevel
{
    OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL;
}