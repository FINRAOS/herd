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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.lang3.ObjectUtils;

import org.finra.dm.model.api.xml.BusinessObjectDataKey;

/**
 * A comparator that compares two business object data keys.
 */
public class BusinessObjectDataKeyComparator implements Comparator<BusinessObjectDataKey>, Serializable
{
    private static final long serialVersionUID = -9075981888098266490L;

    @Override
    public int compare(final BusinessObjectDataKey data1, final BusinessObjectDataKey data2)
    {
        if (data1 == null || data2 == null)
        {
            return data1 == null && data2 == null ? 0 : (data1 == null ? -1 : 1);
        }
        int result = ObjectUtils.compare(data1.getBusinessObjectDefinitionName(), data2.getBusinessObjectDefinitionName());
        if (result != 0)
        {
            return result;
        }
        result = ObjectUtils.compare(data1.getBusinessObjectFormatUsage(), data2.getBusinessObjectFormatUsage());
        if (result != 0)
        {
            return result;
        }
        result = ObjectUtils.compare(data1.getBusinessObjectFormatFileType(), data2.getBusinessObjectFormatFileType());
        if (result != 0)
        {
            return result;
        }
        result = ObjectUtils.compare(data1.getBusinessObjectFormatVersion(), data2.getBusinessObjectFormatVersion());
        if (result != 0)
        {
            return result;
        }
        result = ObjectUtils.compare(data1.getPartitionValue(), data2.getPartitionValue());
        if (result != 0)
        {
            return result;
        }
        return ObjectUtils.compare(data1.getBusinessObjectDataVersion(), data2.getBusinessObjectDataVersion());
    }
}
