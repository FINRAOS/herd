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
package org.finra.herd.swaggergen.test.restControllerProcessor.case1;

import java.io.Serializable;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.swaggergen.test.TestEnum;
import org.finra.herd.swaggergen.test.definitionGenerator.BasicCase;

@RestController
@RequestMapping(value = "/rest", produces = "application/json", consumes = "application/json")
@Api(tags = "CustomTag", hidden = false)
public class Test1RestController
{
    /**
     * Gets basic case.
     * 
     * @param id The ID
     * @return Basic case
     */
    @RequestMapping(value = "/test/{id}", produces = "application/json", consumes = "application/json", method = RequestMethod.GET)
    @ApiOperation(value = "Get basic case")
    public BasicCase get(@PathVariable("id") String id)
    {
        return null;
    }

    /**
     * Lists basic cases
     * 
     * @param query Search query
     * @return Basic case
     */
    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public BasicCase list(@RequestParam("query") String query)
    {
        return null;
    }

    /**
     * Creates a basic case
     * 
     * @param basicCase Basic case to create
     * @return Created basic case
     */
    @RequestMapping(value = "/test", method = RequestMethod.POST)
    public BasicCase create(@RequestBody BasicCase basicCase)
    {
        return null;
    }

    /**
     * Updates basic case
     * 
     * @param id ID of basic case
     * @param basicCase New basic case
     * @return Updated basic case
     */
    @RequestMapping(value = "/test/{id}", consumes = "application/json", method = RequestMethod.PUT)
    public BasicCase update(@PathVariable("id") String id, @RequestBody BasicCase basicCase)
    {
        return null;
    }

    /**
     * Deletes a basic case
     * 
     * @param id ID of basic case
     * @return Deleted basic case
     */
    @RequestMapping(value = "/test/{id}", produces = "application/json", method = RequestMethod.DELETE)
    public BasicCase delete(@PathVariable("id") String id)
    {
        return null;
    }

    @RequestMapping(value = "/test/evaluate", method = RequestMethod.GET)
    public BasicCase evaluate(@RequestParam("number") Integer number, @RequestParam("bigNumber") Long bigNumber, @RequestParam("flag") Boolean flag,
        @RequestParam("choice") TestEnum choice, String mistake, Serializable anotherMistake)
    {
        return null;
    }
}
