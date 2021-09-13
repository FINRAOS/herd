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
package org.finra.herd.swaggergen.test.restControllerProcessor.case3;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.swaggergen.test.definitionGenerator.BasicCase;

@RestController
public class Test3RestController
{
    @RequestMapping(value = "/test/duplicate", method = RequestMethod.GET)
    public BasicCase duplicate()
    {
        return null;
    }

    @RequestMapping(value = "/test/duplicate/{id}", method = RequestMethod.GET)
    public BasicCase duplicate(@PathVariable("id") String id)
    {
        return null;
    }
}
