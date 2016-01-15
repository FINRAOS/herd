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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.servlet.ModelAndView;

import org.finra.herd.core.Command;
import org.finra.herd.ui.constants.UiConstants;
import org.finra.herd.ui.controller.HerdUiControllerAdvice;
import org.finra.herd.ui.controller.UserException;

/**
 * Test the herd Controller Advice.
 */
public class HerdControllerAdviceTest extends AbstractUiTest
{
    @Autowired
    private HerdUiControllerAdvice controllerAdvice;

    private static final String MESSAGE = "This is a test. Please ignore.";

    @Test
    public void testHandleException() throws Exception
    {
        executeWithoutLogging(HerdUiControllerAdvice.class, new Command()
        {
            @Override
            public void execute()
            {
                // Non-user exceptions should only contain the view and no message in the model.
                // Calling handleException will log a stack trace which is normal so don't be concerned if you see it in the logs.
                ModelAndView modelAndView = controllerAdvice.handleException(new Exception(MESSAGE));
                assertTrue(UiConstants.DISPLAY_ERROR_MESSAGE_PAGE.equals(modelAndView.getViewName()));
                assertNull(modelAndView.getModel().get(UiConstants.MODEL_KEY_MESSAGE));
            }
        });
    }

    @Test
    public void testHandleUserException() throws Exception
    {
        // User exceptions should contain a view as well as the message in the model.
        ModelAndView modelAndView = controllerAdvice.handleUserException(new UserException(MESSAGE));
        assertTrue(UiConstants.DISPLAY_ERROR_MESSAGE_PAGE.equals(modelAndView.getViewName()));
        assertTrue(MESSAGE.equals(modelAndView.getModel().get(UiConstants.MODEL_KEY_MESSAGE)));
    }
}
