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
package org.finra.herd.ui.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.util.HtmlUtils;

import org.finra.herd.ui.constants.UiConstants;

/**
 * A general form controller advice that can be used to handle UI controller exceptions.
 */
@ControllerAdvice("org.finra.herd.ui.controller") // Only handle UI exceptions and not REST exceptions in a different base package.
public class HerdUiControllerAdvice
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HerdUiControllerAdvice.class);

    /**
     * Handle all user exceptions by returning to a general error page while displaying the exception message to the user. A "message" model is returned.
     *
     * @param ex the exception being handled.
     *
     * @return the model and view to the error page.
     */
    @ExceptionHandler(UserException.class)
    public ModelAndView handleUserException(UserException ex)
    {
        // Nothing to log here since these are messages meant for the user to see.
        return getDisplayErrorMessageModelAndView(ex.getMessage());
    }

    @ExceptionHandler(AccessDeniedException.class)
    public void handleAccessDeniedException(AccessDeniedException accessDeniedException, HttpServletResponse response) throws IOException
    {
        // This forces the status to be send at the Servlet level, which triggers in the web-container's (ie. Tomcat) default behavior
        response.sendError(HttpStatus.FORBIDDEN.value());
    }

    /**
     * Handle all other controller exceptions by returning to a general error page with a general message. The exception message isn't displayed to the user for
     * security reasons. A "message" model is returned.
     *
     * @param ex the exception being handled.
     *
     * @return the model and view to the error page.
     */
    @ExceptionHandler(Exception.class)
    public ModelAndView handleException(Exception ex)
    {
        LOGGER.error("An error occurred in a UI controller.", ex);
        return getDisplayErrorModelAndView();
    }

    /**
     * Gets a "displayError" model and view with no model present.
     *
     * @return the model and view.
     */
    public static ModelAndView getDisplayErrorModelAndView()
    {
        return getDisplayErrorMessageModelAndView(null);
    }

    /**
     * Gets a "displayErrorMessage" model and view.
     *
     * @param message An optional error message to include in the model. If null, it won't be included in the model. The message will be automatically HTML
     *            escaped.
     *
     * @return the model and view.
     */
    public static ModelAndView getDisplayErrorMessageModelAndView(String message)
    {
        String viewName = UiConstants.DISPLAY_ERROR_MESSAGE_PAGE;
        if (message == null)
        {
            return new ModelAndView(viewName);
        }
        else
        {
            return new ModelAndView(viewName, UiConstants.MODEL_KEY_MESSAGE, HtmlUtils.htmlEscape(message));
        }
    }
}
