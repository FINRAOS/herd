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
package org.finra.herd.service;


import org.finra.herd.model.api.xml.AllowedAttributeValuesCreateRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesDeleteRequest;
import org.finra.herd.model.api.xml.AllowedAttributeValuesInformation;
import org.finra.herd.model.api.xml.AttributeValueListKey;

public interface AllowedAttributeValueService
{
    public AllowedAttributeValuesInformation createAllowedAttributeValues(AllowedAttributeValuesCreateRequest allowedAttributeValuesCreateRequest);

    public AllowedAttributeValuesInformation getAllowedAttributeValues(AttributeValueListKey attributeValueListKey);

    public AllowedAttributeValuesInformation deleteAllowedAttributeValues(AllowedAttributeValuesDeleteRequest allowedAttributeValuesDeleteRequest);
}
