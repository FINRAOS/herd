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

import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.dto.ApplicationUser;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.helper.UserNamespaceAuthorizationHelper;

/**
 * Http headers based Application user builder.
 */
@SuppressFBWarnings(value = "MS_PKGPROTECT", justification = "We will leave CALENDAR_PATTERNS as public since AbstractAppTest needs access to it.")
public class HttpHeaderApplicationUserBuilder implements ApplicationUserBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationUserBuilder.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private UserNamespaceAuthorizationHelper userNamespaceAuthorizationHelper;

    public static final String HTTP_HEADER_USER_ID = "useridHeader";
    public static final String HTTP_HEADER_USER_ID_SUFFIX = "useridSuffixHeader";
    public static final String HTTP_HEADER_FIRST_NAME = "firstNameHeader";
    public static final String HTTP_HEADER_LAST_NAME = "lastNameHeader";
    public static final String HTTP_HEADER_EMAIL = "emailHeader";
    public static final String HTTP_HEADER_ROLES = "rolesHeader";
    public static final String HTTP_HEADER_SESSION_INIT_TIME = "sessionInitTimeHeader";
    public static final String HTTP_HEADER_SESSION_ID = "sessionid";

    // The password expire date format (e.g. 20080202211030Z)
    public static final String CALENDAR_PATTERN_PWD = "yyyyMMddHHmmss'Z'";

    // The session init and expire date format (e.g. Fri, 19 Oct 2007 17:24:09)
    public static final String CALENDAR_PATTERN_SESSION = "EEE, dd MMM yyyy HH:mm:ss";

    // The session init and expire date format with timezone (e.g. Fri, 19 Oct 2007 17:24:09 GMT)
    public static final String CALENDAR_PATTERN_SESSION_TZ = "EEE, dd MMM yyyy HH:mm:ss z";

    // The array of all calendar patterns
    public static final String[] CALENDAR_PATTERNS = {CALENDAR_PATTERN_PWD, CALENDAR_PATTERN_SESSION, CALENDAR_PATTERN_SESSION_TZ};

    @Override
    public ApplicationUser build(HttpServletRequest request)
    {
        return buildUser(getHttpHeaders(request), true);
    }

    @Override
    public ApplicationUser buildNoRoles(HttpServletRequest request)
    {
        // Return the application user without roles.
        return buildUser(getHttpHeaders(request), false);
    }

    /**
     * Creates a map of the HTTP headers contained within the servlet request. The session Id is also placed in the HTTP_HEADER_SESSION_ID key.
     *
     * @param request the HTTP servlet request.
     *
     * @return the map of headers.
     */
    private Map<String, String> getHttpHeaders(HttpServletRequest request)
    {
        // Create a hash map to contain the headers.
        Map<String, String> headerMap = new HashMap<String, String>();

        // Enumerate through each header and store it in the map.
        Enumeration<String> enumeration = request.getHeaderNames();
        while (enumeration.hasMoreElements())
        {
            String header = enumeration.nextElement();
            headerMap.put(header.toLowerCase(), request.getHeader(header));
        }

        // Place the session Id in a special key so it can retrieved later on when building the user.
        headerMap.put(HTTP_HEADER_SESSION_ID, request.getSession().getId());

        // Return the header map.
        return headerMap;
    }

    /**
     * Builds the application user from the header map.
     *
     * @param headerMap the map of headers.
     * @param includeRoles If true, the user's roles will be included. Otherwise, not.
     *
     * @return the application user.
     */
    protected ApplicationUser buildUser(Map<String, String> headerMap, boolean includeRoles)
    {
        LOGGER.debug("Creating Application User From Headers");
        System.out.println("Creating Application User From Headers");

        Map<String, String> headerNames = getHeaderNames();
        // Build the user in pieces.
        ApplicationUser applicationUser = createNewApplicationUser();

        buildUserId(applicationUser, headerMap, headerNames.get(HTTP_HEADER_USER_ID), headerNames.get(HTTP_HEADER_USER_ID_SUFFIX));
        buildFirstName(applicationUser, headerMap, headerNames.get(HTTP_HEADER_FIRST_NAME));
        buildLastName(applicationUser, headerMap, headerNames.get(HTTP_HEADER_LAST_NAME));
        buildEmail(applicationUser, headerMap, headerNames.get(HTTP_HEADER_EMAIL));
        buildSessionId(applicationUser, headerMap, HTTP_HEADER_SESSION_ID);
        buildSessionInitTime(applicationUser, headerMap, headerNames.get(HTTP_HEADER_SESSION_INIT_TIME));
        userNamespaceAuthorizationHelper.buildNamespaceAuthorizations(applicationUser);

        if (includeRoles)
        {
            buildRoles(applicationUser, headerMap, headerNames.get(HTTP_HEADER_ROLES));
        }

        LOGGER.debug("Application user created successfully: " + applicationUser);

        return applicationUser;
    }

    /**
     * A Factory Method for creating a new <code>ApplicationUser</code>. Provide ability for subclasses to override this if a custom user class is required. The
     * custom user class must extend <code>ApplicationUser</code>.
     *
     * @return ApplicationUser (or a subtype)
     */
    protected ApplicationUser createNewApplicationUser()
    {
        return new ApplicationUser(this.getClass());
    }

    /**
     * Builds a user Id.
     *
     * @param applicationUser the application user.
     * @param httpHeaders the HTTP headers.
     * @param userIdHeaderName the header name for the user Id.
     * @param userIdSuffixHeaderName  the header name for the user Id suffix
     */
    protected void buildUserId(ApplicationUser applicationUser, Map<String, String> httpHeaders, String userIdHeaderName, String userIdSuffixHeaderName)
    {
        String userId = getHeaderValueString(userIdHeaderName, httpHeaders);
        if (userId == null)
        {
            throw new IllegalArgumentException("userId is required. No value for userId was found in the header " + userIdHeaderName);
        }

        // append the user id suffix if suffix value is configured in the environment
        String userIdSuffixHeader = getHeaderValueString(userIdSuffixHeaderName, httpHeaders);
        if (userIdSuffixHeader != null)
        {
            userId = userId + "@" + userIdSuffixHeader;
        }
        applicationUser.setUserId(userId);
    }

    protected void buildFirstName(ApplicationUser applicationUser, Map<String, String> httpHeaders, String headerName)
    {
        applicationUser.setFirstName(getHeaderValueString(headerName, httpHeaders));
    }

    protected void buildLastName(ApplicationUser applicationUser, Map<String, String> httpHeaders, String headerName)
    {
        applicationUser.setLastName(getHeaderValueString(headerName, httpHeaders));
    }

    protected void buildEmail(ApplicationUser applicationUser, Map<String, String> httpHeaders, String headerName)
    {
        applicationUser.setEmail(getHeaderValueString(headerName, httpHeaders));
    }

    protected void buildSessionId(ApplicationUser applicationUser, Map<String, String> httpHeaders, String headerName)
    {
        applicationUser.setSessionId(getHeaderValueString(headerName, httpHeaders));
    }

    protected void buildSessionInitTime(ApplicationUser applicationUser, Map<String, String> httpHeaders, String headerName)
    {
        applicationUser.setSessionInitTime(getHeaderValueDate(headerName, httpHeaders));
    }

    /**
     * Parses the header and assigns roles to the given user object. The header value of the given header name will be parsed for roles. See parseRoles(String,
     * Set) method for details on the parsing process.
     *
     * @param applicationUser - the user object to populate the roles to
     * @param httpHeaders - the HTTP headers given in the current request
     * @param headerName - the name of the header containing the roles
     */
    protected void buildRoles(ApplicationUser applicationUser, Map<String, String> httpHeaders, String headerName)
    {
        Set<String> roles = new HashSet<>();
        applicationUser.setRoles(roles);

        String rolesHeaderValue = getHeaderValueString(headerName, httpHeaders);
        if (rolesHeaderValue != null)
        {
            parseRoles(rolesHeaderValue, roles);
        }
        else if (headerName == null || rolesHeaderValue == null)
        {
            //headerName is null, so use multiple roles regex for headers
            for (String header : httpHeaders.keySet())
            {
                parseRole(header, roles);
            }
        }

        /*
         * If we need to have a mechanism to retrieve roles such that a single header represents a unique role, then the extra code to handle this situation
         * could be added here.
         */
    }

    /**
     * <p> Parses the given header value for roles, and adds them to the given collection of roles. </p> <p> The roles are parsed using the configured regex and
     * regex group name retrieved from {@link #getHttpHeaderRoleRegex()} and {@link #getHttpHeaderRoleRegexGroupName()}. </p> <p> The regex is matched against
     * the value until no more matches are found. If the regex group name is set (not empty), then the group with the configured name will be extracted from the
     * match and be set as the role. If the regex group name is not set, then the entire match will be used as the role. Each matching role is then added to the
     * collection of roles supplied. </p> <p> If no regex is configured (regex is empty), then this method does nothing (ie. The app is configured to not use
     * multiple role header). </p>
     *
     * @param value - the string value to parse
     * @param roles - the collection of roles to add the parsed roles to
     *
     * @see <a href="http://www.regular-expressions.info/named.html">How to use named groups</a>
     * @see Pattern
     */
    private void parseRoles(String value, Collection<String> roles)
    {
        String regex = getHttpHeaderRoleRegex();

        // Do nothing if regex is not configured
        if (StringUtils.isNotBlank(regex))
        {
            // Compile the regex
            Pattern pattern = Pattern.compile(regex);

            // Create a matcher from the regex and the given value
            Matcher matcher = pattern.matcher(value);

            String roleRegexGroupName = getHttpHeaderRoleRegexGroupName();

            // Use regex group name if group name is configured
            // Do this outside the loop below to avoid calling isNotBlank() repeatedly
            boolean useRoleRegexGroupName = StringUtils.isNotBlank(roleRegexGroupName);
            while (matcher.find())
            {
                String role;

                // Decide whether to use group or not
                if (useRoleRegexGroupName)
                {
                    // Extract role name based on the regex group name
                    role = matcher.group(roleRegexGroupName);
                }
                else
                {
                    // Use entire match as the role
                    role = matcher.group();
                }

                // If a role was matched
                if (role != null)
                {
                    // Add the role to the result
                    roles.add(role);
                }
            }
        }
    }

    /**
     * <p> Parses the given header value for roles, and adds them to the given collection of roles. </p> <p> The roles are parsed using the configured regex
     * retrieved from {@link #getHttpHeaderRolesRegex()}. </p> <p> The regex is matched against the value until no more matches are found. </p> <p> If no regex
     * is configured (regex is empty), then this method does nothing (ie. The app is configured to use  multiple role header). </p>
     *
     * @param value - the string value to parse
     * @param roles - the collection of roles to add the parsed roles to
     *
     * @see <a href="http://www.regular-expressions.info/named.html">How to use named groups</a>
     * @see Pattern
     */
    private void parseRole(String value, Collection<String> roles)
    {
        String regex = getHttpHeaderRolesRegex();

        // Do nothing if regex is not configured
        if (StringUtils.isNotBlank(regex))
        {
            // Compile the regex
            Pattern pattern = Pattern.compile(regex);

            // Create a matcher from the regex and the given value
            Matcher matcher = pattern.matcher(value);

            while (matcher.find())
            {
                String role = matcher.group(1);
                roles.add(role);
            }
        }
    }

    /**
     * Gets an HTTP header string value from the map of headers for the specified header name. If the header value is blank, null will be returned in its place.
     * The header name is case-insensitive, but will be converted to lower-case before retrieving the header value. The returned header value is trimmed.
     *
     * @param headerName the HTTP header name to get.
     * @param httpHeaders the HTTP headers.
     *
     * @return the HTTP header value.
     */
    protected String getHeaderValueString(String headerName, Map<String, String> httpHeaders)
    {
        String headerNameLocal = headerName;

        if (headerNameLocal != null)
        {
            headerNameLocal = headerNameLocal.toLowerCase();
        }

        String headerValue = httpHeaders.get(headerNameLocal);
        if (StringUtils.isBlank(headerValue))
        {
            headerValue = null;
        }
        else
        {
            headerValue = headerValue.trim();
        }
        return headerValue;
    }

    /**
     * Gets an HTTP header date value from the map of headers for the specified header name. If the header value is blank or can't be converted to a Date, null
     * will be returned in its place.
     *
     * @param headerName the HTTP header name to get.
     * @param httpHeaders the HTTP headers.
     *
     * @return the HTTP header value.
     */
    public Date getHeaderValueDate(String headerName, Map<String, String> httpHeaders)
    {
        // Default the return value to null.
        Date dateValue = null;

        // Get the string value for the header name.
        String stringValue = getHeaderValueString(headerName, httpHeaders);

        // Try to convert it to a Date if possible
        if (stringValue != null)
        {
            try
            {
                dateValue = DateUtils.parseDate(stringValue, CALENDAR_PATTERNS);
            }
            catch (Exception ex)
            {
                // The value couldn't be converted to a Date so leave the dateValue null.
                dateValue = null;
            }
        }

        // Return the calendar value.
        return dateValue;
    }

    /**
     * Gets the header names from configuration in the current environment.
     *
     * @return map of header name key to header value
     */
    private Map<String, String> getHeaderNames()
    {
        Map<String, String> headerNames = new HashMap<>();

        List<String> headers = herdStringHelper.splitStringWithDefaultDelimiter(getSecurityHeaderNames());

        for (String header : headers)
        {
            String[] keyValueString = header.split("=");

            headerNames.put(keyValueString[0], keyValueString[1]);
        }

        return headerNames;
    }

    /**
     * Gets the delimited security header names.
     *
     * @return header names
     */
    private String getSecurityHeaderNames()
    {
        return getProperty(ConfigurationValue.SECURITY_HTTP_HEADER_NAMES);
    }

    /**
     * Gets the regex to use to parse a role from a string which may contain multiple roles. May return empty string, in which case the application should not
     * attempt to apply role parsing.
     *
     * @return regex
     */
    private String getHttpHeaderRoleRegex()
    {
        return getProperty(ConfigurationValue.SECURITY_HTTP_HEADER_ROLE_REGEX);
    }

    /**
     * Gets the regex to use to parse a role headers. May return empty string, in which case the application should not attempt to apply role parsing.
     *
     * @return regex
     */
    private String getHttpHeaderRolesRegex()
    {
        return getProperty(ConfigurationValue.SECURITY_HTTP_HEADER_ROLES_REGEX);
    }

    /**
     * Gets the group name of a regex match which represents a role. May return empty string, in which case the application should not use named groups.
     *
     * @return regex group name
     */
    private String getHttpHeaderRoleRegexGroupName()
    {
        return getProperty(ConfigurationValue.SECURITY_HTTP_HEADER_ROLE_REGEX_GROUP);
    }

    /**
     * Gets the property value from the current environment. If no value is configured, uses whatever default is configured in the given configurationValue.
     *
     * @param configurationValue - the configuration value to retrieve
     *
     * @return the value in the environment
     */
    private String getProperty(ConfigurationValue configurationValue)
    {
        return configurationHelper.getProperty(configurationValue);
    }
}
