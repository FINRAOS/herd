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
package org.finra.dm.ui;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.WebUtils;

/**
 * A servlet filter that logs incoming HTTP requests. This approach is similar to the Spring CommonsRequestLoggingFilter, but is customized to ensure that the
 * full request body is always read and logged. In addition, this filter only has the concept of "before" request logging.
 */
public class RequestLoggingFilter extends OncePerRequestFilter
{
    private static final Logger LOGGER = Logger.getLogger(RequestLoggingFilter.class);

    public static final String DEFAULT_LOG_MESSAGE_PREFIX = "HTTP Request [";
    public static final String DEFAULT_LOG_MESSAGE_SUFFIX = "]";
    private static final Integer DEFAULT_MAX_PAYLOAD_LENGTH = null; // Default to unlimited.

    private boolean includeQueryString = true;
    private boolean includeClientInfo = true;
    private boolean includePayload = true;
    private Integer maxPayloadLength = DEFAULT_MAX_PAYLOAD_LENGTH;
    private String logMessagePrefix = DEFAULT_LOG_MESSAGE_PREFIX;
    private String logMessageSuffix = DEFAULT_LOG_MESSAGE_SUFFIX;

    /**
     * Set whether or not the query string should be included in the log message.
     */
    public void setIncludeQueryString(boolean includeQueryString)
    {
        this.includeQueryString = includeQueryString;
    }

    /**
     * Return whether or not the query string should be included in the log message.
     */
    protected boolean isIncludeQueryString()
    {
        return this.includeQueryString;
    }

    /**
     * Set whether or not the client address and session id should be included in the log message.
     */
    public void setIncludeClientInfo(boolean includeClientInfo)
    {
        this.includeClientInfo = includeClientInfo;
    }

    /**
     * Return whether or not the client address and session id should be included in the log message.
     */
    protected boolean isIncludeClientInfo()
    {
        return this.includeClientInfo;
    }

    /**
     * Set whether or not the request payload (body) should be included in the log message.
     */
    public void setIncludePayload(boolean includePayload)
    {
        this.includePayload = includePayload;
    }

    /**
     * Return whether or not the request payload (body) should be included in the log message.
     */
    protected boolean isIncludePayload()
    {
        return includePayload;
    }

    /**
     * Sets the maximum length of the payload body to be included in the log message. Default (i.e. null) is unlimited characters.
     */
    public void setMaxPayloadLength(Integer maxPayloadLength)
    {
        this.maxPayloadLength = maxPayloadLength;
    }

    /**
     * Return the maximum length of the payload body to be included in the log message.
     */
    protected Integer getMaxPayloadLength()
    {
        return maxPayloadLength;
    }

    /**
     * Set the value that should be prepended to the log message.
     */
    public void setLogMessagePrefix(String logMessagePrefix)
    {
        this.logMessagePrefix = logMessagePrefix;
    }

    /**
     * Set the value that should be appended to the log message.
     */
    public void setLogMessageSuffix(String logMessageSuffix)
    {
        this.logMessageSuffix = logMessageSuffix;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException
    {
        HttpServletRequest requestLocal = request;

        // Determine if this is the first request or not. We only want to wrap the request to log on the first request.
        boolean isFirstRequest = !isAsyncDispatch(requestLocal);
        if (isFirstRequest)
        {
            requestLocal = new RequestLoggingFilterWrapper(requestLocal);
        }

        // Move onto the next filter while wrapping the request with our own custom logging class.
        filterChain.doFilter(requestLocal, response);
    }

    /**
     * A request wrapper that logs incoming requests.
     */
    public class RequestLoggingFilterWrapper extends HttpServletRequestWrapper
    {
        private byte[] payload = null;
        private BufferedReader reader;

        /**
         * Constructs a request logging filter wrapper.
         *
         * @param request the request to wrap.
         *
         * @throws IOException if any problems were encountered while reading from the stream.
         */
        public RequestLoggingFilterWrapper(HttpServletRequest request) throws IOException
        {
            // Perform super class processing.
            super(request);

            // Only grab the payload if debugging is enabled. Otherwise, we'd always be pre-reading the entire payload for no reason which cause a slight
            // performance degradation for no reason.
            if (LOGGER.isDebugEnabled())
            {
                // Read the original payload into the payload variable.
                InputStream inputStream = null;
                try
                {
                    // Get the input stream.
                    inputStream = request.getInputStream();
                    if (inputStream != null)
                    {
                        // Read the payload from the input stream.
                        payload = IOUtils.toByteArray(request.getInputStream());
                    }
                }
                finally
                {
                    if (inputStream != null)
                    {
                        try
                        {
                            inputStream.close();
                        }
                        catch (IOException iox)
                        {
                            LOGGER.warn("Unable to close request input stream.", iox);
                        }
                    }
                }

                // Log the request.
                logRequest(request);
            }
        }

        /**
         * Log the request message.
         *
         * @param request the request.
         */
        protected void logRequest(HttpServletRequest request)
        {
            StringBuilder message = new StringBuilder();

            // Append the log message prefix.
            message.append(logMessagePrefix);

            // Append the URI.
            message.append("uri=").append(request.getRequestURI());

            // Append the query string if present.
            if (isIncludeQueryString() && StringUtils.hasText(request.getQueryString()))
            {
                message.append('?').append(request.getQueryString());
            }

            // Append the HTTP method.
            message.append(";method=").append(request.getMethod());

            // Append the client information.
            if (isIncludeClientInfo())
            {
                // The client remote address.
                String client = request.getRemoteAddr();
                if (StringUtils.hasLength(client))
                {
                    message.append(";client=").append(client);
                }

                // The HTTP session information.
                HttpSession session = request.getSession(false);
                if (session != null)
                {
                    message.append(";session=").append(session.getId());
                }

                // The remote user information.
                String user = request.getRemoteUser();
                if (user != null)
                {
                    message.append(";user=").append(user);
                }
            }

            // Get the request payload.
            String payloadString = "";
            try
            {
                if (payload != null && payload.length > 0)
                {
                    payloadString = new String(payload, 0, payload.length, getCharacterEncoding());
                }
            }
            catch (UnsupportedEncodingException e)
            {
                payloadString = "[Unknown]";
            }

            // Append the request payload if present.
            if (isIncludePayload() && StringUtils.hasLength(payloadString))
            {
                String sanitizedPayloadString = payloadString;
                /*
                 * TODO Remove this logic once proper way of securing sensitive data is implemented.
                 * Replaces the payload if it contains the word "password" for requests to jobDefinitions and jobs.
                 */
                if (request.getRequestURI().endsWith("/jobDefinitions") || request.getRequestURI().endsWith("/jobs")
                    || request.getRequestURI().endsWith("/jobs/signal"))
                {
                    Pattern pattern = Pattern.compile("password", Pattern.CASE_INSENSITIVE);
                    if (pattern.matcher(payloadString).find())
                    {
                        sanitizedPayloadString = "<hidden because it may contain sensitive information>";
                    }
                }
                /*
                 * Limit logged payload length if max length is set
                 */
                else if (getMaxPayloadLength() != null)
                {
                    sanitizedPayloadString = payloadString.substring(0, Math.min(payloadString.length(), getMaxPayloadLength()));
                }

                message.append(";payload=").append(sanitizedPayloadString);
            }

            // Append the log message suffix.
            message.append(logMessageSuffix);

            // Log the actual message.
            LOGGER.debug(message.toString());
        }

        @Override
        public ServletInputStream getInputStream() throws IOException
        {
            if (payload == null)
            {
                // If no payload is present (i.e. debug logging isn't enabled), then perform the standard super class functionality.
                return super.getInputStream();
            }
            else
            {
                return new ServletInputStream()
                {
                    final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload);

                    public int read() throws IOException
                    {
                        // Read bytes from the previously read payload.
                        return byteArrayInputStream.read();
                    }
                };
            }
        }

        @Override
        public int getContentLength()
        {
            if (payload == null)
            {
                return super.getContentLength();
            }
            else
            {
                return payload.length;
            }
        }

        @Override
        public String getCharacterEncoding()
        {
            String enc = super.getCharacterEncoding();
            return (enc != null ? enc : WebUtils.DEFAULT_CHARACTER_ENCODING);
        }

        @Override
        public BufferedReader getReader() throws IOException
        {
            if (payload == null)
            {
                return super.getReader();
            }
            else
            {
                if (reader == null)
                {
                    this.reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(payload), getCharacterEncoding()));
                }
                return reader;
            }
        }
    }
}
