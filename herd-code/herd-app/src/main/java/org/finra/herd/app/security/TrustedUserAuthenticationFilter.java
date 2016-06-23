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
package org.finra.herd.app.security;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationDetailsSource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.filter.GenericFilterBean;

/**
 * A Spring pre-authentication filter that builds a trusted user based on a SpEL expression environment variable.
 */
public class TrustedUserAuthenticationFilter extends GenericFilterBean
{
    @Autowired
    private SecurityHelper securityHelper;

    /**
     * The authentication manager to authenticate with.
     */
    private AuthenticationManager authenticationManager = null;

    /**
     * An application user builder that knows how to build an application user.
     */
    private ApplicationUserBuilder applicationUserBuilder;

    /**
     * The authentication details source. The default is a WebAuthenticationDetailsSource.
     */
    private AuthenticationDetailsSource<HttpServletRequest, WebAuthenticationDetails> authenticationDetailsSource = new WebAuthenticationDetailsSource();

    public TrustedUserAuthenticationFilter(AuthenticationManager authenticationManager, ApplicationUserBuilder applicationUserBuilder)
    {
        this.authenticationManager = authenticationManager;
        this.applicationUserBuilder = applicationUserBuilder;
    }

    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST",
        justification = "The ServletRequest is cast to an HttpServletRequest which is always the case since all requests use the HTTP protocol.")
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
    {
        doHttpFilter((HttpServletRequest) servletRequest, (HttpServletResponse) servletResponse, filterChain);
    }

    /**
     * doFilter implementation for an HTTP request and response.
     *
     * @param request the HTTP servlet request.
     * @param response the HTTP servlet response.
     * @param chain the filter chain.
     *
     * @throws IOException if an I/O error occurs.
     * @throws ServletException if a servlet error occurs.
     */
    public void doHttpFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException
    {
        // Check if security is enabled
        // If security is not enabled, perform allow as trusted user.
        if (!securityHelper.isSecurityEnabled(request))
        {
            // If authentication is not there or is not of trusted user type.
            PreAuthenticatedAuthenticationToken authRequest = new PreAuthenticatedAuthenticationToken(applicationUserBuilder.build(request), "N/A");
            authRequest.setDetails(authenticationDetailsSource.buildDetails(request));
            Authentication authResult = authenticationManager.authenticate(authRequest);

            // The authentication returned so it was successful.
            SecurityContextHolder.getContext().setAuthentication(authResult);
        }

        chain.doFilter(request, response);
    }
}
