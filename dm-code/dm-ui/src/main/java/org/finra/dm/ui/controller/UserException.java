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
package org.finra.dm.ui.controller;

/**
 * An exception whose message will be displayed to the user.
 */
public class UserException extends RuntimeException
{
    public UserException()
    {
        super();
    }

    public UserException(String message)
    {
        super(message);
    }

    public UserException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public UserException(Throwable cause)
    {
        super(cause);
    }
}
