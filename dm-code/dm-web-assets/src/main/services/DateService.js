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
(function()
{
    'use strict';

    angular.module('dm').service('DateService', DateService);

    /**
     * A simple wrapper for moment. This wrapper exists mainly to mock how the
     * current date is retrieved in unit tests such that the tests are not
     * dependent to the time at which the test is running.
     */
    function DateService()
    {

    }

    /**
     * Returns the current time.
     * 
     * @returns {moment} - current time
     */
    DateService.prototype.now = function()
    {
        return moment();
    };

    /**
     * Formats the given date into the given format as defined by
     * http://momentjs.com/docs/#/displaying/format/.
     * 
     * Returns null if given date is falsy.
     * 
     * @param {Date|Moment} date - The date to format
     * @param {String} format - The date format pattern
     * @returns {String} - The formatted date
     */
    DateService.prototype.formatDate = function(date, format)
    {
        return (date && moment(date).format(format)) || null;
    };

    /**
     * Parses the given string using the given format as defined by
     * http://momentjs.com/docs/#/parsing/string-format/.
     * 
     * Returns null if given string is falsy or string has an invalid format.
     * 
     * @param {String} string - The string to parse into date
     * @param {String} format - The format of the date that the string is
     *        expected to be in
     * @returns {Date} The parsed date
     */
    DateService.prototype.parseDate = function(string, format)
    {
        var date = null;
        if (string)
        {
            var parsedDate = moment(string, format);
            if (parsedDate.isValid())
            {
                date = parsedDate.toDate();
            }
        }
        return date;
    };

})();