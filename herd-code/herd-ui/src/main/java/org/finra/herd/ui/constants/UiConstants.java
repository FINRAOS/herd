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
package org.finra.herd.ui.constants;

/**
 * Constants for the UI tier.
 */
public final class UiConstants
{
    // Page names.
    public static final String DISPLAY_BUILD_INFO_PAGE = "displayBuildInfo";
    public static final String DISPLAY_ERROR_MESSAGE_PAGE = "displayErrorMessage";
    public static final String DISPLAY_INFO_MESSAGE_PAGE = "displayInfoMessage";
    public static final String DISPLAY_HERD_UI_PAGE = "herd-ui";

    // URL's.
    public static final String REST_URL_BASE = "/rest";
    public static final String DISPLAY_BUILD_INFO_URL = "/" + DISPLAY_BUILD_INFO_PAGE;
    public static final String DISPLAY_ERROR_MESSAGE_URL = "/" + DISPLAY_ERROR_MESSAGE_PAGE;
    public static final String DISPLAY_INFO_MESSAGE_URL = "/" + DISPLAY_INFO_MESSAGE_PAGE;
    public static final String DISPLAY_HERD_UI_URL = "/" + DISPLAY_HERD_UI_PAGE;

    // Model keys.
    public static final String MODEL_KEY_MESSAGE = "message";
    public static final String MODEL_KEY_BUILD_INFORMATION = "buildInformation";

    private UiConstants()
    {
        // Prevent classes from instantiating.
    }
}
