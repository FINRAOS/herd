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
package org.finra.herd.dao.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.finra.herd.dao.UrlOperations;

/**
 * Mock implementation of URL operations.
 */
public class MockUrlOperationsImpl implements UrlOperations
{
    public static final String MOCK_JSON_STRING = "{\"json\":\"string\"}";

    public static final String MOCK_URL_JSON_PARSE_EXCEPTION = "http://mock.url.json.parse.exception";

    public static final String MOCK_URL_MALFORMED_URL_EXCEPTION = "mock_url_malformed_url_exception";

    public static final String MOCK_URL_VALID = "http://mock.url.valid";

    @Override
    public InputStream openStream(URL url, Proxy proxy) throws IOException
    {
        String urlRepresentation = url.toString();

        if (MOCK_URL_JSON_PARSE_EXCEPTION.equals(urlRepresentation))
        {
            return new ByteArrayInputStream(new byte[0]);
        }
        else
        {
            return new ByteArrayInputStream(MOCK_JSON_STRING.getBytes(StandardCharsets.UTF_8));
        }
    }
}
