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

    angular.module('herd').controller('DateRangeController', DateRangeController);

    /**
     * A controller for DateRangeDirective.
     * 
     * @class
     * @memberof controllers
     */
    function DateRangeController()
    {
        var vm = this;

        /**
         * A flag which indicates whether the date picker for the start date is
         * open.
         * 
         * @member {Boolean} isStartDateOpen
         * @memberof controllers.DateRangeController
         */
        vm.isStartDateOpen = false;

        /**
         * A flag which indicates whether the date picker for the end date is
         * open.
         * 
         * @member {Boolean} isEndDateOpen
         * @memberof controllers.DateRangeController
         */
        vm.isEndDateOpen = false;
    }

    /**
     * Opens the date picker for the start date. Stops propagation of the given
     * event. Hides the date picker for end date if opened.
     * 
     * @param {$event} $event - Event which triggered the action
     */
    DateRangeController.prototype.openStartDate = function($event)
    {
        var vm = this;

        $event.preventDefault();
        $event.stopPropagation();

        vm.isStartDateOpen = true;
        vm.isEndDateOpen = false;
    };

    /**
     * Opens the date picker for the end date. Stops propagation of the given
     * event. Hides the date picker for end start if opened.
     * 
     * @param {$event} $event - Event which triggered the action
     */
    DateRangeController.prototype.openEndDate = function($event)
    {
        var vm = this;

        $event.preventDefault();
        $event.stopPropagation();

        vm.isStartDateOpen = false;
        vm.isEndDateOpen = true;
    };

    /**
     * Handles event when start date has changed.
     * 
     * Triggers change(), if registered. Otherwise does nothing.
     */
    DateRangeController.prototype.onStartDateChanged = function()
    {
        triggerDateChanged(this);
    };

    /**
     * Handles event when end date has changed.
     * 
     * Triggers change(), if registered. Otherwise does nothing.
     */
    DateRangeController.prototype.onEndDateChanged = function()
    {
        triggerDateChanged(this);
    };

    /**
     * Calls change() if the given vm has the function registered. Otherwise,
     * does nothing.
     * 
     * @param {controllers.DateRangeController} vm - The controller
     */
    function triggerDateChanged(vm)
    {
        if (vm.change)
        {
            vm.change();
        }
    }
})();