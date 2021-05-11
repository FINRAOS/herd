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
package org.finra.herd.app;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/*
 * A servlet filter that invalidates session after precessing request.
 */
public class SessionInvalidationFilter implements Filter
{
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
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException
    {
        // Retrieve the session.
        HttpSession session = null;

        try
        {
            // Retrieve the session.
            session = ((HttpServletRequest) servletRequest).getSession();

            // Call the next filter in the chain.
            chain.doFilter(servletRequest, servletResponse);
        }
        finally
        {
            if (session != null)
            {
                session.invalidate();
            }
        }
    }
}
