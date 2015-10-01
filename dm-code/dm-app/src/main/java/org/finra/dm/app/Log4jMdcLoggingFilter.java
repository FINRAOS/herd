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
package org.finra.dm.app;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.MDC;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;

/*
 * A servlet filter that places Log4J contextual information in the map diagnostic context.  This can then be used in the log4j.xml configuration to log this
 * contextual information in the log files.
 */
public class Log4jMdcLoggingFilter implements Filter
{
    // MDC property keys.  These can be referenced in a log4j.xml configuration.
    private static final String USER_ID_KEY = "uid";
    private static final String SESSION_ID_KEY = "sid";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
        // Nothing to do in init method.
    }

    @Override
    public void destroy()
    {
        // Nothing to do.
    }

    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST",
        justification = "The ServletRequest is cast to an HttpServletRequest which is always the case since all requests use the HTTP protocol.")
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException
    {
        try
        {
            // Store the session Id.
            HttpSession session = ((HttpServletRequest) servletRequest).getSession();
            MDC.put(SESSION_ID_KEY, "sessionId=" + session.getId());

            String userId = "";
            // Try to extract the actual username from the security context.
            if (SecurityContextHolder.getContext().getAuthentication() != null)
            {
                Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
                if (principal instanceof User)
                {
                    User user = (User) principal;
                    userId = user.getUsername();
                }
                else
                {
                    userId = principal.toString();
                }
            }

            MDC.put(USER_ID_KEY, "userId=" + userId);

            // Call the next filter in the chain.
            chain.doFilter(servletRequest, servletResponse);
        }
        finally
        {
            // Remove the MDC properties to ensure they don't accidentally get used by anybody else.
            MDC.remove(USER_ID_KEY);
            MDC.remove(SESSION_ID_KEY);
        }
    }
}
