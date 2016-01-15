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

    /**
     * A directive to work around the issue in angular bootstrap's date parsing
     * logic detailed in https://github.com/angular-ui/bootstrap/issues/956.
     * 
     * Following closely with the fix in http://developer.the-hideout.de/?p=119.
     * 
     * TODO Unfortunately, we are using moment to do the parsing, which uses a
     * different formatting pattern than angular's date. The format is
     * hard-coded here, but ideally should be retrieved from the datepicker's
     * format.
     */
    angular.module('herd').directive('herdDatePickerFix', function()
    {
        return {
            priority : 1,
            restrict : 'EA',
            require : 'ngModel',
            link : function(scope, element, attrs, ngModel)
            {
                // var format = attrs.datepickerPopup; here is the date
                // format, now how to parse using this!
                var model = ngModel;

                ngModel.$parsers.unshift(function(viewValue)
                {
                    var value = viewValue;
                    if (typeof viewValue === 'string')
                    {
                        // hard-coded format here
                        value = moment(viewValue, 'YYYY-MM-DD').toDate();
                    }
                    model.$setValidity('date', true);
                    model.$setViewValue(value);
                    return value;
                });
            }
        };
    });
})();