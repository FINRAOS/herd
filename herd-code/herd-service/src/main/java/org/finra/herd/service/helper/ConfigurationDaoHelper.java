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

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.ConfigurationDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.MethodNotAllowedException;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * Helper for configuration related operations which require DAO.
 */
@Component
public class ConfigurationDaoHelper
{
    @Autowired
    private ConfigurationDao configurationDao;

    @Autowired
    private HerdStringHelper herdStringHelper;

    /**
     * Checks if the method name is not allowed against the configuration.
     *
     * @param methodName the method name
     *
     * @throws org.finra.herd.model.MethodNotAllowedException if requested method is not allowed.
     */
    public void checkNotAllowedMethod(String methodName) throws MethodNotAllowedException
    {
        boolean needToBlock = false;
        ConfigurationEntity configurationEntity = configurationDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey());
        if (configurationEntity != null && StringUtils.isNotBlank(configurationEntity.getValueClob()))
        {
            List<String> methodsToBeBlocked = herdStringHelper.splitStringWithDefaultDelimiter(configurationEntity.getValueClob());
            needToBlock = methodsToBeBlocked.contains(methodName);
        }

        if (needToBlock)
        {
            throw new MethodNotAllowedException("The requested method is not allowed.");
        }
    }
}
