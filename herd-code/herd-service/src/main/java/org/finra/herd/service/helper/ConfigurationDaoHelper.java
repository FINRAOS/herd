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

import javax.xml.bind.JAXBException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.ConfigurationDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.XmlHelper;
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

    @Autowired
    private XmlHelper xmlHelper;

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

    /**
     * Method used to return character large object configuration values
     *
     * @param configurationKey the configuration key used to obtain the CLOB
     *
     * @return String clob
     */
    public String getClobProperty(String configurationKey)
    {
        String clob = "";
        ConfigurationEntity configurationEntity = configurationDao.getConfigurationByKey(configurationKey);
        if (configurationEntity != null && StringUtils.isNotBlank(configurationEntity.getValueClob()))
        {
            clob = configurationEntity.getValueClob();
        }

        return clob;
    }

    /**
     * Returns JAXB object unmarshalled from the specified character large object configuration value.
     *
     * @param classType the class type of JAXB element
     * @param configurationKey the configuration key used to obtain the CLOB
     * @param <T> the class type.
     *
     * @return the JAXB object or null if specified value is not configured
     */
    @SuppressWarnings("unchecked")
    public <T> T getXmlClobPropertyAndUnmarshallToObject(Class<T> classType, String configurationKey)
    {
        String xml = getClobProperty(configurationKey);

        if (StringUtils.isNotBlank(xml))
        {
            try
            {
                return xmlHelper.unmarshallXmlToObject(classType, xml);
            }
            catch (JAXBException e)
            {
                throw new IllegalStateException(String.format("Failed to unmarshall \"%s\" configuration value to %s.", configurationKey, classType.getName()),
                    e);
            }
        }
        else
        {
            return null;
        }
    }
}
