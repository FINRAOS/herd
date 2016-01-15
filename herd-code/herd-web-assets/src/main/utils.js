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
var herd = {
    utils : {}
};

(function()
{
    'use strict';

    /**
     * Returns true if both strings equals ignoring case, false otherwise. This
     * function is null safe.
     * 
     * @param {String} str1 - string value
     * @param {String} str2 - string value
     * @returns {Boolean} true or false
     */
    herd.utils.equalsIgnoreCase = function(str1, str2)
    {
        str1 = str1 && str1.toUpperCase();
        str2 = str2 && str2.toUpperCase();

        return str1 === str2;
    };

    /**
     * Returns true if the given value exists in the given string.
     * 
     * @param {String} str
     * @param {String} searchValue
     * @returns {Boolean}
     */
    herd.utils.containsIgnoreCase = function(str, searchValue)
    {
        str = str.toUpperCase();
        searchValue = searchValue.toUpperCase();
        return str.indexOf(searchValue) >= 0;
    };
})();