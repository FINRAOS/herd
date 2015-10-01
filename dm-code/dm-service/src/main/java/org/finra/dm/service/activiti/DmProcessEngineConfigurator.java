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
package org.finra.dm.service.activiti;

import org.activiti.engine.cfg.ProcessEngineConfigurator;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.activiti.engine.impl.variable.LongStringType;
import org.activiti.engine.impl.variable.StringType;
import org.activiti.engine.impl.variable.VariableType;
import org.activiti.engine.impl.variable.VariableTypes;
import org.springframework.stereotype.Component;

/**
 * Activiti process engine configurator used to update Activiti configuration before the engine is built.
 */
@Component
public class DmProcessEngineConfigurator implements ProcessEngineConfigurator
{
    @Override
    public void beforeInit(ProcessEngineConfigurationImpl configuration)
    {
        // Nothing to do before init.
    }

    @Override
    public void configure(ProcessEngineConfigurationImpl processEngineConfiguration)
    {
        // There is an issue when using Activiti with Oracle. When workflow variables have a length greater then 2000 and smaller than 4001,
        // Activiti tries to store that as a String column, but the Oracle database column length (ACT_RU_VARIABLE.TEXT_, ACT_HI_VARINST.TEXT_) is 2000.
        // Since Activiti uses NVARCHAR as the column type which requires 2 bytes for every character, the maximum size Oracle allows for the column is
        // 2000 (i.e. 4000 bytes).

        // Replace the StringType and LongType to store greater than 2000 length variables as blob.
        VariableTypes variableTypes = processEngineConfiguration.getVariableTypes();

        VariableType stringType = new StringType(2000);
        int indexToInsert = replaceVariableType(variableTypes, stringType, 0);

        VariableType longStringType = new LongStringType(2001);
        replaceVariableType(variableTypes, longStringType, ++indexToInsert);
    }

    @Override
    public int getPriority()
    {
        // Using 0 as priority as this is the only configurator we have.
        return 0;
    }

    /**
     * Inserts the variableType in variableTypes at the given index. If variableType already exists, then it replaces the type at the location where it already
     * exists.
     *
     * @param variableTypes the variableTypes
     * @param variableTypeToReplace the variableType to insert
     * @param indexToInsert the index to insert variableType
     *
     * @return the index where variableType was inserted.
     */
    private int replaceVariableType(VariableTypes variableTypes, VariableType variableTypeToReplace, int indexToInsert)
    {
        int indexToInsertReturn = indexToInsert;

        VariableType existingVariableType = variableTypes.getVariableType(variableTypeToReplace.getTypeName());
        if (existingVariableType != null)
        {
            indexToInsertReturn = variableTypes.getTypeIndex(existingVariableType.getTypeName());

            variableTypes.removeType(existingVariableType);
        }
        variableTypes.addType(variableTypeToReplace, indexToInsertReturn);

        return indexToInsertReturn;
    }
}