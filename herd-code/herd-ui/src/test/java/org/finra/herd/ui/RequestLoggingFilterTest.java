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
package org.finra.herd.ui;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.web.util.WebUtils;

import org.finra.herd.core.helper.LogLevel;
/**
 * Test driver for the RequestLoggingFilter class. Since the filter's main functionality logs messages which is difficult to test, the majority of test cases
 * will simply ensure that exceptions are not thrown under various configuration approaches.
 */
public class RequestLoggingFilterTest extends AbstractUiTest
{
    // Test payload content.
    private static final String PAYLOAD_CONTENT = "Test Body";

    @Before
    public void setup() throws Exception
    {
        // Turn on debug logging which is what enables the logging by the filter.
        setLogLevel(RequestLoggingFilter.class, LogLevel.DEBUG);

        super.setup();
    }

    @Test
    public void testDoFilter() throws Exception
    {
        // Run the filter.
        createFilter().doFilter(createServletRequest(), createServletResponse(), createFilterChain());
    }

    @Test
    public void testDoFilterNoDebug() throws Exception
    {
        // Turn on info logging which will disable the core functionality of the filter (i.e. no logging).
        setLogLevel(RequestLoggingFilter.class, LogLevel.INFO);

        // Run the filter.
        createFilter().doFilter(createServletRequest(), createServletResponse(), createFilterChain());
    }

    @Test
    public void testDoFilterAllOptionsFalse() throws Exception
    {
        RequestLoggingFilter requestLoggingFilter = new RequestLoggingFilter();
        requestLoggingFilter.setIncludeClientInfo(false);
        requestLoggingFilter.setIncludePayload(false);
        requestLoggingFilter.setIncludeQueryString(false);

        // Run the filter.
        requestLoggingFilter.doFilter(createServletRequest(), createServletResponse(), createFilterChain());
    }

    @Test
    public void testDoFilterMaxPayloadLength() throws Exception
    {
        RequestLoggingFilter requestLoggingFilter = new RequestLoggingFilter();
        requestLoggingFilter.setMaxPayloadLength(4);

        // Run the filter.
        requestLoggingFilter.doFilter(createServletRequest(), createServletResponse(), createFilterChain());
    }

    @Test
    public void testDoFilterNoPayload() throws Exception
    {
        MockHttpServletRequest request = createServletRequest();
        request.setContent(null);

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), createFilterChain());
    }

    @Test
    public void testDoFilterEmptyContentType() throws Exception
    {
        MockHttpServletRequest request = createServletRequest();
        request.setContentType("");

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), createFilterChain());
    }

    @Test(timeout = 1000)
    @Ignore
    public void testDoFilterLongSingleLineXMLPayload() throws Exception
    {
        String fileName = "long_filter_xml_payload.txt";
        File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
        byte[] payload = Files.readAllBytes(file.toPath());

        MockHttpServletRequest request = createServletRequest();
        request.setContent(payload);
        request.setContentType(ContentType.APPLICATION_XML.toString());

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), createFilterChain());
    }

    @Test(timeout = 1000)
    @Ignore
    public void testDoFilterLongSingleLineJSONPayload() throws Exception
    {
        String fileName = "long_filter_json_payload.txt";
        File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
        byte[] payload = Files.readAllBytes(file.toPath());

        MockHttpServletRequest request = createServletRequest();
        request.setContent(payload);
        request.setContentType(ContentType.APPLICATION_JSON.toString());

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), createFilterChain());
    }

    @Test
    public void testDoFilterReadInputStreamFromFilterChainWithPayload() throws Exception
    {
        FilterChain filterChain = new MockFilterChain()
        {
            public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException
            {
                String payload = IOUtils.toString(request.getInputStream());
                assertEquals(payload, PAYLOAD_CONTENT);
            }
        };

        // Run the filter.
        createFilter().doFilter(createServletRequest(), createServletResponse(), filterChain);
    }

    @Test
    public void testDoFilterReadInputStreamFromFilterChainWithNoPayload() throws Exception
    {
        FilterChain filterChain = new MockFilterChain()
        {
            public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException
            {
                String payload = IOUtils.toString(request.getInputStream());
                assertEquals("", payload);
            }
        };

        MockHttpServletRequest request = createServletRequest();
        request.setContent(null);

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), filterChain);
    }

    @Test
    public void testDoFilterReadInputStreamFromFilterChainWithNoPayloadNoDebugLevel() throws Exception
    {
        // Turn on info logging which will disable the core functionality of the filter (i.e. no logging).
        setLogLevel(RequestLoggingFilter.class, LogLevel.INFO);

        FilterChain filterChain = new MockFilterChain()
        {
            public void doFilter(ServletRequest request, ServletResponse response) throws IOException, ServletException
            {
                String payload = IOUtils.toString(request.getInputStream());
                assertEquals("", payload);
            }
        };

        MockHttpServletRequest request = createServletRequest();
        request.setContent(null);

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), filterChain);
    }

    @Test
    public void testDoFilterNoClient() throws Exception
    {
        MockHttpServletRequest request = createServletRequest();
        request.setRemoteAddr(null);

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), createFilterChain());
    }

    @Test
    public void testDoFilterNullInputStream() throws Exception
    {
        MockHttpServletRequest request = new MockHttpServletRequest()
        {
            public ServletInputStream getInputStream()
            {
                return null;
            }
        };

        // Run the filter.
        createFilter().doFilter(request, createServletResponse(), createFilterChain());
    }

    /**
     * Test to get the coverage for unused implemented methods of RequestLoggingFilterWrapper.
     */
    @Test
    public void testRequestLoggingFilterWrapper() throws Exception
    {
        HttpServletRequest servletRequest = createServletRequest();
        servletRequest.setCharacterEncoding(WebUtils.DEFAULT_CHARACTER_ENCODING);
        RequestLoggingFilter requestLoggingFilter = new RequestLoggingFilter();
        RequestLoggingFilter.RequestLoggingFilterWrapper wrapper = requestLoggingFilter.new RequestLoggingFilterWrapper(servletRequest);
        wrapper.logRequest(servletRequest);
        wrapper.getContentLength();
        wrapper.getCharacterEncoding();
        wrapper.getReader();
        wrapper.getReader();
    }

    private MockHttpServletRequest createServletRequest()
    {
        MockHttpServletRequest request = new MockHttpServletRequest(null, "/test");
        request.setQueryString("param=value");
        request.setMethod("POST");
        MockHttpSession session = new MockHttpSession();
        request.setContent(PAYLOAD_CONTENT.getBytes());
        request.setSession(session);
        request.setRemoteUser("Test Remote User");
        return request;
    }

    private ServletResponse createServletResponse()
    {
        return new MockHttpServletResponse();
    }

    private FilterChain createFilterChain()
    {
        return new MockFilterChain();
    }

    private RequestLoggingFilter createFilter()
    {
        RequestLoggingFilter requestLoggingFilter = new RequestLoggingFilter();
        requestLoggingFilter.setIncludeClientInfo(true);
        requestLoggingFilter.setIncludePayload(true);
        requestLoggingFilter.setIncludeQueryString(true);
        requestLoggingFilter.setLogMessagePrefix("Log Message: [");
        requestLoggingFilter.setLogMessageSuffix("]");
        requestLoggingFilter.setMaxPayloadLength(null);
        return requestLoggingFilter;
    }
}
