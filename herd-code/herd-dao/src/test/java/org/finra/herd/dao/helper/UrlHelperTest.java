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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.AbstractDaoTest;
import org.finra.herd.dao.impl.MockUrlOperationsImpl;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * This class tests functionality within the UrlHelper class.
 */
public class UrlHelperTest extends AbstractDaoTest
{
    @Autowired
    private UrlHelper urlHelper;

    @Test
    public void testParseJsonObjectFromUrl()
    {
        assertEquals(MockUrlOperationsImpl.MOCK_JSON_STRING, urlHelper.parseJsonObjectFromUrl(MockUrlOperationsImpl.MOCK_URL_VALID).toJSONString());
    }

    @Test
    public void testParseJsonObjectFromUrlJsonParseException()
    {
        try
        {
            urlHelper.parseJsonObjectFromUrl(MockUrlOperationsImpl.MOCK_URL_JSON_PARSE_EXCEPTION);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Failed to parse JSON object from the URL: url=\"%s\"", MockUrlOperationsImpl.MOCK_URL_JSON_PARSE_EXCEPTION),
                e.getMessage());
        }
    }

    @Test
    public void testParseJsonObjectFromUrlMalformedURLException()
    {
        try
        {
            urlHelper.parseJsonObjectFromUrl(MockUrlOperationsImpl.MOCK_URL_MALFORMED_URL_EXCEPTION);
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Failed to read JSON from the URL: url=\"%s\"", MockUrlOperationsImpl.MOCK_URL_MALFORMED_URL_EXCEPTION), e.getMessage());
        }
    }

    @Test
    public void testParseJsonObjectFromUrlWithProxySettings() throws Exception
    {
        // Override the configuration to specify proxy settings.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HTTP_PROXY_HOST.getKey(), HTTP_PROXY_HOST);
        overrideMap.put(ConfigurationValue.HTTP_PROXY_PORT.getKey(), HTTP_PROXY_PORT);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            assertEquals(MockUrlOperationsImpl.MOCK_JSON_STRING, urlHelper.parseJsonObjectFromUrl(MockUrlOperationsImpl.MOCK_URL_VALID).toJSONString());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testParseJsonObjectFromUrlWithProxySettingsNoProxyPort() throws Exception
    {
        // Override the configuration to specify proxy settings without proxy port.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.HTTP_PROXY_HOST.getKey(), HTTP_PROXY_HOST);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            assertEquals(MockUrlOperationsImpl.MOCK_JSON_STRING, urlHelper.parseJsonObjectFromUrl(MockUrlOperationsImpl.MOCK_URL_VALID).toJSONString());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }
}
