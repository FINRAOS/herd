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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An Activiti specific filter that enables us to serve up all Activiti resources from within our own created single "activiti" folder.
 */
public class DmActivitiFilter implements Filter
{
    private List<String> activitiResourceCoreSubFolders;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
        // Create a list of all core sub-folders that all Activiti resources are contained within.
        activitiResourceCoreSubFolders = Arrays.asList("/api", "/diagram-viewer", "/editor", "/explorer", "/libs");
    }

    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST",
        justification = "The ServletRequest is cast to an HttpServletRequest which is always the case since all requests use the HTTP protocol.")
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException
    {
        // Activiti explorer has several static resources that need to be in specific directories.
        // We will serve them up within our own created "activit" core folder.

        HttpServletRequest httpServletRequest = (HttpServletRequest) request;

        // Get the path after the context path and get the index of the first slash after any root slash.
        String urlPathWithoutContextPath = httpServletRequest.getRequestURI().substring(httpServletRequest.getContextPath().length());
        int indexOfSlashAfterFirstSubFolder = urlPathWithoutContextPath.indexOf('/', 1);

        // Get the first sub-folder of the URL (i.e. the part up to the first slash). If a slash wasn't found,
        // then return the entire path since only one folder exists.
        String firstSubFolder;
        if (indexOfSlashAfterFirstSubFolder > 0)
        {
            firstSubFolder = urlPathWithoutContextPath.substring(0, indexOfSlashAfterFirstSubFolder);
        }
        else
        {
            firstSubFolder = urlPathWithoutContextPath;
        }

        // If the request is within one of the Activiti core sub-folders, then forward to the same path within our own consolidated "activit" sub-folder.
        // Otherwise, just go to the default servlet.
        if (activitiResourceCoreSubFolders.contains(firstSubFolder))
        {
            request.getRequestDispatcher("/activiti" + urlPathWithoutContextPath).forward(request, response);
        }
        else
        {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy()
    {
        // Nothing to do in destroy method.
    }
}