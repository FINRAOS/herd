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
package org.finra.herd.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BuildInformation;
import org.finra.herd.model.api.xml.TimeoutValidationResponse;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles general herd REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Application")
public class HerdRestController extends HerdBaseController
{
    @Autowired
    private BuildInformation buildInformation;
    private final Integer maxWaitForSeconds = 1800;

    /**
     * Gets the build information.
     *
     * @return the build information.
     */
    @ApiOperation(value = "Gets the build information")
    @RequestMapping(value = "/buildInfo", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUILD_INFO_GET)
    public BuildInformation getBuildInfo()
    {
        return buildInformation;
    }

    /**
     * Validates infrastructure timeouts.
     * @param waitForSeconds number of seconds to wait that falls between 0 and 1800 inclusively
     * @return the timeout validation response.
     * @throws InterruptedException If the internal wait fails.
     */
    @ApiOperation(value = "Validates infrastructure timeouts", hidden = true)
    @RequestMapping(value = "/timeoutValidation", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_TIMEOUT_VALIDATION_GET)
    public TimeoutValidationResponse getTimeoutValidation(@RequestParam(value = "waitForSeconds") Integer waitForSeconds) throws InterruptedException
    {
        Assert.isTrue(waitForSeconds != null, "waitForSeconds query parameter is required.");
        Assert.isTrue(waitForSeconds >= 0 && waitForSeconds <= maxWaitForSeconds,
            "Specified value \"" + waitForSeconds +"\" does not fall within the range of 0 to 1800 seconds.");
        Thread.sleep(waitForSeconds * 1000L);
        return new TimeoutValidationResponse("Successfully waited for " + waitForSeconds + " seconds.");
    }
}
