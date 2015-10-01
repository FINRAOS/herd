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
package org.finra.dm.service.helper;

import java.util.HashMap;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Factory class for EMR step helper generators.
 */
@Component
public class EmrStepHelperFactory implements InitializingBean
{
    @Autowired
    private ApplicationContext applicationContext;

    private Map<String, EmrStepHelper> emrStepHelperMap;

    /**
     * This method returns the EMR step helper for the given class.
     *
     * @param stepType the step class name
     *
     * @return EmrStepHelper the EMR step helper
     */
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
        justification = "This is a false positive. afterPropertiesSet is called before this method which will ensure emrStepHelperMap is not null.")
    public EmrStepHelper getStepHelper(String stepType)
    {
        EmrStepHelper stepHelper = emrStepHelperMap.get(stepType);
        if (stepHelper == null)
        {
            throw new IllegalArgumentException("No supported EMR step helper found for stepType: " + stepType + ".");
        }
        return stepHelper;
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        emrStepHelperMap = new HashMap<>();

        // Add all the available EMR step helpers.
        Map<String, EmrStepHelper> helperBeanMap = applicationContext.getBeansOfType(EmrStepHelper.class);
        for (EmrStepHelper stepHelper : helperBeanMap.values())
        {
            emrStepHelperMap.put(stepHelper.getStepRequestType(), stepHelper);
            emrStepHelperMap.put(stepHelper.getStepType(), stepHelper);
        }
    }
}