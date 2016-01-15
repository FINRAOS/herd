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
package org.finra.herd.service.helper;

import java.util.HashMap;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.BusinessObjectDataDdlOutputFormatEnum;

/**
 * Factory class for DDL generators.
 */
@Component
public class DdlGeneratorFactory implements InitializingBean
{
    @Autowired
    private ApplicationContext applicationContext;

    private Map<BusinessObjectDataDdlOutputFormatEnum, DdlGenerator> ddlGeneratorMap;

    /**
     * This method returns the DDL generator for the given output format.
     *
     * @param outputFormat the output format
     *
     * @return DdlGenerator the DDL generator
     */
    @SuppressFBWarnings(value = "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR",
        justification = "This is a false positive. afterPropertiesSet is called before this method which will ensure ddlGeneratorMap is not null.")
    public DdlGenerator getDdlGenerator(BusinessObjectDataDdlOutputFormatEnum outputFormat)
    {
        DdlGenerator generator = ddlGeneratorMap.get(outputFormat);
        if (generator == null)
        {
            throw new UnsupportedOperationException(
                "No supported DDL generator found for output format: " + (outputFormat == null ? null : outputFormat.value()) + ".");
        }
        return generator;
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        ddlGeneratorMap = new HashMap<>();

        // Add all the available DDL generators.
        Map<String, DdlGenerator> ddlGeneratorBeanMap = applicationContext.getBeansOfType(DdlGenerator.class);
        for (DdlGenerator ddlGenerator : ddlGeneratorBeanMap.values())
        {
            ddlGeneratorMap.put(ddlGenerator.getDdlOutputFormat(), ddlGenerator);
        }
    }
}
