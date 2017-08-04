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

package org.finra.dm.service.activiti.task;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.stereotype.Component;

/**
 * Old Activiti Java delegate.
 *
 * @deprecated use the class defined in the org.finra.herd.service.activiti.task package.
 */
@Component
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "This is a deprecated class that extends the class of the same name in a different Java package. It will be removed when all users " +
        "migrate to the new Java package.")
public class CheckBusinessObjectDataAvailabilityMultiplePartitions
    extends org.finra.herd.service.activiti.task.CheckBusinessObjectDataAvailabilityMultiplePartitions
{
}
