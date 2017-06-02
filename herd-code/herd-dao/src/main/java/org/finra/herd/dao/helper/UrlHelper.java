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
package org.finra.herd.dao.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.UrlOperations;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * A helper class for URL functionality.
 */
@Component
public class UrlHelper
{
    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private UrlOperations urlOperations;

    /**
     * Reads JSON from a specified URL.
     *
     * @param url the url
     *
     * @return the JSON object
     */
    public JSONObject parseJsonObjectFromUrl(String url)
    {
        try
        {
            // Get proxy information.
            Proxy proxy;
            String httpProxyHost = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_HOST);
            Integer httpProxyPort = configurationHelper.getProperty(ConfigurationValue.HTTP_PROXY_PORT, Integer.class);
            if (StringUtils.isNotBlank(httpProxyHost) && httpProxyPort != null)
            {
                proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(httpProxyHost, httpProxyPort));
            }
            else
            {
                proxy = Proxy.NO_PROXY;
            }

            // Open an input stream as per specified URL.
            InputStream inputStream = urlOperations.openStream(new URL(url), proxy);

            try
            {
                // Parse the JSON object from the input stream.
                JSONParser jsonParser = new JSONParser();
                return (JSONObject) jsonParser.parse(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            }
            catch (ParseException e)
            {
                throw new IllegalArgumentException(String.format("Failed to parse JSON object from the URL: url=\"%s\"", url), e);
            }
            finally
            {
                inputStream.close();
            }
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(String.format("Failed to read JSON from the URL: url=\"%s\"", url), e);
        }
    }
}
