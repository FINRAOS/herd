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
package org.finra.herd.dao;

import org.finra.herd.model.jpa.OnDemandPriceEntity;

public interface OnDemandPriceDao extends BaseJpaDao
{
    /**
     * Returns the on-demand price with the specified region and instance type. Returns {@code null} if no on-demand price is found. Throws an exception when
     * more than 1 on-demand price is found.
     *
     * @param region The on-demand price's region.
     * @param instanceType The on-demand price's instance type.
     *
     * @return The on-demand price.
     */
    public OnDemandPriceEntity getOnDemandPrice(String region, String instanceType);
}
