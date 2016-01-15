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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.web.servlet.ModelAndView;

import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.ui.constants.UiConstants;

/**
 * This class tests various functionality within the herd UI controller.
 */
public class HerdControllerTest extends AbstractUiTest
{
    private static Logger logger = Logger.getLogger(HerdControllerTest.class);

    @Test
    public void testDisplayBuildInfo() throws Exception
    {
        // Get a model and view back from the UI controller displayBuildInfo method and ensure it is correct and contains the correct model data.
        ModelAndView modelAndView = herdController.displayBuildInfo();
        assertNotNull(modelAndView);
        assertEquals(UiConstants.DISPLAY_BUILD_INFO_PAGE, modelAndView.getViewName());
        BuildInformation buildInformation = (BuildInformation) modelAndView.getModel().get(UiConstants.MODEL_KEY_BUILD_INFORMATION);
        assertNotNull(buildInformation);
        assertNotNull(buildInformation.getBuildDate());
        logger.info(buildInformation);
    }

    @Test
    public void testDisplayInfoMessageWithMessage() throws Exception
    {
        // Call the controller with a test message.
        String testMessage = "testMessage";
        ModelAndView modelAndView = herdController.displayInfoMessage(testMessage);

        // Verify that the view name and the passed in message are both present.
        assertNotNull(modelAndView);
        assertEquals(UiConstants.DISPLAY_INFO_MESSAGE_PAGE, modelAndView.getViewName());
        String resultMessage = (String) modelAndView.getModel().get(UiConstants.MODEL_KEY_MESSAGE);
        assertTrue(testMessage.equals(resultMessage));
    }

    @Test
    public void testDisplayInfoMessageWithNoMessage() throws Exception
    {
        // Call the controller with no message.
        ModelAndView modelAndView = herdController.displayInfoMessage(null);

        // Verify that the view name exists, but that no message is present.
        assertNotNull(modelAndView);
        assertEquals(UiConstants.DISPLAY_INFO_MESSAGE_PAGE, modelAndView.getViewName());
        assertNull(modelAndView.getModel().get(UiConstants.MODEL_KEY_MESSAGE));
    }

    @Test
    public void testDisplayErrorMessageWithMessage() throws Exception
    {
        // Call the controller with a test message.
        String testMessage = "testMessage";
        ModelAndView modelAndView = herdController.displayErrorMessage(testMessage);

        // Verify that the view name and the passed in message are both present.
        assertNotNull(modelAndView);
        assertEquals(UiConstants.DISPLAY_ERROR_MESSAGE_PAGE, modelAndView.getViewName());
        String resultMessage = (String) modelAndView.getModel().get(UiConstants.MODEL_KEY_MESSAGE);
        assertTrue(testMessage.equals(resultMessage));
    }

    @Test
    public void testDisplayErrorMessageWithNoMessage() throws Exception
    {
        // Call the controller with no message.
        ModelAndView modelAndView = herdController.displayErrorMessage(null);

        // Verify that the view name exists, but that no message is present.
        assertNotNull(modelAndView);
        assertEquals(UiConstants.DISPLAY_ERROR_MESSAGE_PAGE, modelAndView.getViewName());
        assertNull(modelAndView.getModel().get(UiConstants.MODEL_KEY_MESSAGE));
    }
}
