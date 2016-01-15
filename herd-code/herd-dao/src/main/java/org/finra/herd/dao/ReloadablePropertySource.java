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
package org.finra.herd.dao;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.log4j.Logger;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.StringUtils;

/**
 * A property source that will possibly re-load itself each time a property is requested. A reload will take place if the configured refresh interval has
 * elapsed. A refresh interval of 0 will cause the properties to refresh every time a property is requested.
 * <p/>
 * If a property is loaded with the key org.finra.herd.dao.ReloadablePropertiesSource.refreshIntervalSecs, it will be used as a way to override the previously
 * configured refresh interval.
 */
public class ReloadablePropertySource extends MapPropertySource
{
    private static final Logger LOGGER = Logger.getLogger(ReloadablePropertySource.class);

    // The configuration that can read properties.
    protected Configuration configuration;

    // The last time the properties were refreshed.
    protected long lastRefreshTime;

    // The interval in milliseconds to wait before refreshing the properties. Defaults to 0 (i.e. always refresh).
    protected long refreshIntervalMillis = 0;

    // The number of milliseconds in a second.
    private static final int MILLISECONDS_IN_A_SECOND = 1000;

    /**
     * The override key for the refresh interval seconds.
     */
    public static final String REFRESH_INTERVAL_SECS_OVERRIDE_KEY = ReloadablePropertySource.class.getName() + ".refreshIntervalSecs";

    /**
     * Constructs the object with a default refresh interval of 60 seconds.
     *
     * @param name the name of the property source.
     * @param source the properties.
     * @param configuration the configuration that knows how to read properties.
     */
    public ReloadablePropertySource(String name, Properties source, Configuration configuration)
    {
        this(name, source, configuration, 60);
    }

    /**
     * Constructs the object with all parameters specified.
     *
     * @param name the name of the property source.
     * @param source the properties.
     * @param configuration the configuration that knows how to read properties.
     * @param refreshIntervalSecs the refresh interval in seconds to wait before refreshing the properties when a property is requested.
     */
    @SuppressWarnings("unchecked")
    public ReloadablePropertySource(String name, Properties source, Configuration configuration, long refreshIntervalSecs)
    {
        super(name, (Map) source);
        this.configuration = configuration;
        this.refreshIntervalMillis = refreshIntervalSecs * MILLISECONDS_IN_A_SECOND;
        updateLastRefreshTime();
        updateRefreshInterval();
        LOGGER.info("A refresh interval of " + refreshIntervalSecs + " seconds has been configured.");
    }

    /**
     * Gets a property by name while possibly refreshing the properties if needed.
     *
     * @param name the property name.
     *
     * @return the property value.
     */
    @Override
    public Object getProperty(String name)
    {
        // Refresh the properties before returning the value.
        refreshPropertiesIfNeeded();
        return this.source.get(name);
    }

    /**
     * Refreshes the properties from the configuration if it's time to.
     */
    @SuppressWarnings("unchecked")
    protected void refreshPropertiesIfNeeded()
    {
        // Ensure we update the properties in a synchronized fashion to avoid possibly corrupting the properties.
        synchronized (this)
        {
            // See if it's time to refresh the properties (i.e. the elapsed time is greater than the configured refresh interval).
            LOGGER.debug(
                "Checking if properties need to be refreshed. Current time is " + System.currentTimeMillis() + " and last refresh time is " + lastRefreshTime +
                    " which is a delta of " + (System.currentTimeMillis() - lastRefreshTime) + ".");

            if (System.currentTimeMillis() - lastRefreshTime >= refreshIntervalMillis)
            {
                // Enough time has passed so refresh the properties.
                LOGGER.debug("Refreshing properties.");

                // Get the latest properties from the configuration.
                Properties properties = ConfigurationConverter.getProperties(configuration);

                // Log the properties we just retrieved from the configuration.
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("New properties just retrieved.");
                    for (Map.Entry<Object, Object> entry : properties.entrySet())
                    {
                        LOGGER.debug("Key [" + entry.getKey() + "] = " + entry.getValue());
                    }
                }

                // Update our property sources properties with the ones just read by clearing and adding in the new ones since the "source" is final.
                this.source.clear();
                this.source.putAll((Map) properties);

                // Log the properties we have in our property source.
                if (LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Updated reloadable properties.");
                    for (Object key : source.keySet())
                    {
                        LOGGER.debug("Key [" + key + "] = " + properties.get(key));
                    }
                }

                // Update the last refresh time and refresh interval.
                updateLastRefreshTime();
                updateRefreshInterval();

                LOGGER.debug("The properties have been refreshed from the configuration.");
            }
        }
    }

    /**
     * Updates the last refresh time to the current time.
     */
    private void updateLastRefreshTime()
    {
        // Update when the properties were last refreshed to the current time.
        this.lastRefreshTime = System.currentTimeMillis();
        LOGGER.debug("Last refresh time set to " + lastRefreshTime);
    }

    /**
     * Updates the refresh interval if a property with the override key was found. Otherwise, the previously configured value will remain.
     */
    private void updateRefreshInterval()
    {
        // Get the property based on the override key.
        String refreshIntervalSecsString = (String) this.source.get(REFRESH_INTERVAL_SECS_OVERRIDE_KEY);

        // If a value was found, try to update the refresh interval.
        if (StringUtils.hasText(refreshIntervalSecsString))
        {
            try
            {
                long newRefreshIntervalMillis = Long.parseLong(refreshIntervalSecsString) * MILLISECONDS_IN_A_SECOND;
                if (newRefreshIntervalMillis != refreshIntervalMillis)
                {
                    refreshIntervalMillis = newRefreshIntervalMillis;
                    LOGGER.info("A new refresh interval of " + refreshIntervalSecsString + " seconds has been configured.");
                }
            }
            catch (NumberFormatException ex)
            {
                // A value was found, but is invalid (e.g. could be a non-number). Just log a warning and keep old value.
                LOGGER.warn("Invalid refresh interval seconds override value found: " + refreshIntervalSecsString + ". Value must be a valid number.");
            }
        }
    }
}
