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
import java.util.Date;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.hibernate.exception.JDBCConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationDetailsSource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.AuthenticationTrustResolver;
import org.springframework.security.authentication.AuthenticationTrustResolverImpl;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.WebAttributes;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.filter.GenericFilterBean;

import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.SecurityUserWrapper;

/**
 * A Spring pre-authentication filter that works with Http headers.
 */
public class HttpHeaderAuthenticationFilter extends GenericFilterBean
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpHeaderAuthenticationFilter.class);

    @Autowired
    private SecurityHelper securityHelper;

    /**
     * An authentication trust resolver.
     */
    private AuthenticationTrustResolver authenticationTrustResolver = new AuthenticationTrustResolverImpl();

    /**
     * The authentication details source. The default is a WebAuthenticationDetailsSource.
     */
    private AuthenticationDetailsSource<HttpServletRequest, WebAuthenticationDetails> authenticationDetailsSource = new WebAuthenticationDetailsSource();

    /**
     * The authentication manager to authenticate with.
     */
    private AuthenticationManager authenticationManager = null;

    /**
     * An application user builder that knows how to build an application user.
     */
    private ApplicationUserBuilder applicationUserBuilder;

    public HttpHeaderAuthenticationFilter(AuthenticationManager authenticationManager, ApplicationUserBuilder applicationUserBuilder)
    {
        this.authenticationManager = authenticationManager;
        this.applicationUserBuilder = applicationUserBuilder;
    }

    /**
     * Perform pre-authentication processing. This method delegates to doHttpFilter.
     *
     * @param servletRequest the servlet request.
     * @param servletResponse the servlet response.
     * @param filterChain the filter chain.
     *
     * @throws IOException if an IO exception was encountered.
     * @throws ServletException if a servlet exception was encountered.
     */
    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST",
        justification = "The ServletRequest is cast to an HttpServletRequest which is always the case since all requests use the HTTP protocol.")
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException
    {
        doHttpFilter((HttpServletRequest) servletRequest, (HttpServletResponse) servletResponse, filterChain);
    }

    /**
     * Perform pre-authentication processing for Http Servlets.
     *
     * @param servletRequest the servlet request.
     * @param servletResponse the servlet response.
     * @param filterChain the filter chain.
     *
     * @throws IOException when an exception is thrown executing the next filter in chain.
     * @throws ServletException if a servlet exception was encountered.
     */
    public void doHttpFilter(HttpServletRequest servletRequest, HttpServletResponse servletResponse, FilterChain filterChain)
        throws IOException, ServletException
    {
        if (securityHelper.isSecurityEnabled(servletRequest))
        {
            // Build an application user from the current HTTP headers.
            ApplicationUser applicationUserNoRoles;
            try
            {
                applicationUserNoRoles = applicationUserBuilder.buildNoRoles(servletRequest);
            }
            catch (JDBCConnectionException jdbcConnectionException)
            {
                // database connection is not available
                invalidateUser(servletRequest, false);
                throw new IllegalStateException("No Database Connection Available", jdbcConnectionException);
            }
            catch (Exception ex)
            {
                applicationUserNoRoles = null;
            }

            if (applicationUserNoRoles == null)
            {
                // We were unable to find/build an application user (i.e. the user isn't logged on) so invalidate the current user if one exists.
                processUserNotLoggedIn(servletRequest);
            }
            else
            {
                LOGGER.debug("Current user Id: " + applicationUserNoRoles.getUserId() + ", Session Init Time: " + applicationUserNoRoles.getSessionInitTime());
                LOGGER.debug("User is logged in.");
                invalidateUser(servletRequest, false);

                // If the user is logged in, but no user information is in the security context holder, then perform the authentication
                // (which will automatically load the user information for us). This flow can be caused when a new user logs for the first time or
                // when a different user just logged in.
                authenticateUser(servletRequest);
            }
        }

        // Continue on to the next filter in the chain.
        filterChain.doFilter(servletRequest, servletResponse);
    }

    /**
     * Creates the user based on the given request, and puts the user into the security context. Throws if authentication fails.
     *
     * @param servletRequest {@link HttpServletRequest} containing the user's request.
     */
    private void authenticateUser(HttpServletRequest servletRequest)
    {
        try
        {
            // Setup the authentication request and perform the authentication. Perform the authentication based on the fully built user.
            PreAuthenticatedAuthenticationToken preAuthenticatedAuthenticationToken =
                new PreAuthenticatedAuthenticationToken(applicationUserBuilder.build(servletRequest), "N/A");
            preAuthenticatedAuthenticationToken.setDetails(authenticationDetailsSource.buildDetails(servletRequest));
            Authentication authentication = authenticationManager.authenticate(preAuthenticatedAuthenticationToken);

            // The authentication returned so it was successful.
            successfulAuthentication(authentication);
        }
        catch (AuthenticationException e)
        {
            // An authentication exception was thrown so authentication failed.
            unsuccessfulAuthentication(servletRequest, e);

            // Throw an exception so we don't continue since there is some problem (e.g. user profile doesn't
            // exist for the logged in user or it couldn't be retrieved).
            throw e;
        }
    }

    /**
     * Perform processing when the user is not logged in.
     *
     * @param servletRequest the servlet request.
     */
    protected void processUserNotLoggedIn(HttpServletRequest servletRequest)
    {
        LOGGER.debug("No user is currently logged in.");
        // No user is currently logged in.
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if ((authentication != null) && (!authenticationTrustResolver.isAnonymous(authentication)))
        {
            // A previous user was logged in so invalidate the session and null out the user.
            LOGGER.debug("A previous user with userIdentity " + getExistingUserId() + " was logged in so invalidating the user.");
            invalidateUser(servletRequest, true);
        }
    }

    /**
     * Invalidates a user by invalidating their session and removing them from the security context holder.
     *
     * @param servletRequest the servlet request.
     * @param invalidateSession flag to indicate whether the Http Session should be invalidated or not.
     */
    protected void invalidateUser(HttpServletRequest servletRequest, boolean invalidateSession)
    {
        if (invalidateSession)
        {
            HttpSession session = servletRequest.getSession(false);
            if (session != null)
            {
                LOGGER.debug("Invalidating the session.");
                session.invalidate();
            }
        }
        LOGGER.debug("Clearing the security context.");
        SecurityContextHolder.clearContext();
    }

    /**
     * Gets the existing user Id.
     *
     * @return the existing user Id, session Id, or null if no existing user is present.
     */
    protected String getExistingUserId()
    {
        String existingUserId = null;
        ApplicationUser applicationUser = getExistingUser();
        if (applicationUser != null)
        {
            existingUserId = applicationUser.getUserId();
        }
        return existingUserId;
    }

    /**
     * Gets the existing session init time.
     *
     * @return the existing session init time or null if no existing user is present.
     */
    protected Date getExistingSessionInitTime()
    {
        Date existingSessionInitTime = null;
        ApplicationUser applicationUser = getExistingUser();
        if (applicationUser != null)
        {
            existingSessionInitTime = applicationUser.getSessionInitTime();
        }
        return existingSessionInitTime;
    }

    /**
     * Gets the existing user.
     *
     * @return the existing user or null if no existing user is present.
     */
    protected ApplicationUser getExistingUser()
    {
        ApplicationUser applicationUser = null;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null)
        {
            SecurityUserWrapper securityUserWrapper = (SecurityUserWrapper) authentication.getPrincipal();
            if (securityUserWrapper != null)
            {
                applicationUser = securityUserWrapper.getApplicationUser();
                LOGGER.trace("Existing Application User: " + applicationUser);
                return applicationUser;
            }
        }
        return applicationUser;
    }

    /**
     * Puts the <code>Authentication</code> instance returned by the authentication manager into the secure context.
     *
     * @param authResult the authorization result.
     */
    protected void successfulAuthentication(Authentication authResult)
    {
        // Save the authentication information in the security context holder.
        LOGGER.debug("Authentication success: " + authResult);
        SecurityContextHolder.getContext().setAuthentication(authResult);
    }

    /**
     * Ensures the authentication object in the secure context is set to null when authentication fails.
     *
     * @param servletRequest the servlet request.
     * @param authenticationException the authentication exception.
     */
    protected void unsuccessfulAuthentication(HttpServletRequest servletRequest, AuthenticationException authenticationException)
    {
        LOGGER.debug("Authentication failure: ", authenticationException);
        invalidateUser(servletRequest, false);
        servletRequest.getSession().setAttribute(WebAttributes.AUTHENTICATION_EXCEPTION, authenticationException);
    }
}
