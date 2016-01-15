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
package org.finra.herd.swaggergen.test.restControllerProcessor.case2;

import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.swaggergen.test.definitionGenerator.BasicCase;

@RestController
public class Test2RestController
{
    /**
     * Get basic case
     * 
     * @param id Basic case ID
     * @return Basic case
     * @see A reference
     */
    @RequestMapping(value = "/test/{id}", method = RequestMethod.GET)
    public BasicCase get(@PathVariable("id") String id)
    {
        return null;
    }

    @RequestMapping(value = "/test/hidden", method = RequestMethod.GET)
    @ApiOperation(value = "i am hidden", hidden = true)
    public BasicCase hidden(@PathVariable("id") String id)
    {
        return null;
    }

    public void notAnEndpoint()
    {
    }

    @RequestMapping(value = "/test/missingMethods")
    public void missingMethods()
    {
    }

    @RequestMapping(method = RequestMethod.GET)
    public void missingUris()
    {
    }

    @RequestMapping(value = "/test/multipleMethods", method = {RequestMethod.GET, RequestMethod.POST})
    public BasicCase multipleMethods()
    {
        return null;
    }

    @RequestMapping(value = {"/test/multipleUris1", "/test/multipleUris2"}, method = RequestMethod.GET)
    public BasicCase multipleUris()
    {
        return null;
    }
}
