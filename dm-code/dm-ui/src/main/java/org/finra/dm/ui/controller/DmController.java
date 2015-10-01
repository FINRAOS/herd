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
package org.finra.dm.ui.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.util.HtmlUtils;

import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.api.xml.BuildInformation;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The UI controller that will handle UI requests for various pages.
 */
@Controller
public class DmController
{
    @Autowired
    private BuildInformation buildInformation;

    /**
     * Displays the build information.
     *
     * @return the model and view.
     */
    @RequestMapping(UiConstants.DISPLAY_BUILD_INFO_URL)
    public ModelAndView displayBuildInfo()
    {
        // Create the model and view with the necessary model objects populated for the screen.
        ModelAndView modelAndView = new ModelAndView(UiConstants.DISPLAY_BUILD_INFO_PAGE);
        modelAndView.addObject(UiConstants.MODEL_KEY_BUILD_INFORMATION, buildInformation);

        // Return the model and view for the page.
        return modelAndView;
    }

    /**
     * Displays an informational message.
     *
     * @param message the message to display.
     *
     * @return the model and view.
     */
    @RequestMapping(UiConstants.DISPLAY_INFO_MESSAGE_URL)
    public ModelAndView displayInfoMessage(@RequestParam(UiConstants.MODEL_KEY_MESSAGE) String message)
    {
        String viewName = UiConstants.DISPLAY_INFO_MESSAGE_PAGE;
        if (message == null)
        {
            return new ModelAndView(viewName);
        }
        else
        {
            return new ModelAndView(viewName, UiConstants.MODEL_KEY_MESSAGE, HtmlUtils.htmlEscape(message));
        }
    }

    @RequestMapping(UiConstants.DISPLAY_ERROR_MESSAGE_URL)
    public ModelAndView displayErrorMessage(@RequestParam(UiConstants.MODEL_KEY_MESSAGE) String message)
    {
        return DmUiControllerAdvice.getDisplayErrorMessageModelAndView(message);
    }

    @Secured(SecurityFunctions.FN_DISPLAY_DM_UI)
    @RequestMapping(UiConstants.DISPLAY_DM_UI_URL)
    public ModelAndView displayDmUi()
    {
        return new ModelAndView(UiConstants.DISPLAY_DM_UI_PAGE);
    }
}
