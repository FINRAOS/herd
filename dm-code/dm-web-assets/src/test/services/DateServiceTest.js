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

    describe('DateServiceTest', function()
    {
        var DateService;

        beforeEach(module('dm'));
        beforeEach(inject(function($injector)
        {
            DateService = $injector.get('DateService');
        }));

        /*
         * Cannot validate that the result is the current date as that is
         * dependent on how the current time is processed. Instead we will
         * verify that the returned value is a moment object.
         */
        describe('now()', function()
        {
            beforeEach(function()
            {
                this.date = DateService.now();
            });

            it('returns a moment object', function()
            {
                expect(this.date).toBeTruthy();
                expect(this.date.format).toBeTruthy();
                expect(this.date.format('YYYY-MM-DD')).toBeTruthy();
            });
        });

        describe('formatDate()', function()
        {
            describe('when moment date is passed', function()
            {
                beforeEach(function()
                {
                    this.result = DateService.formatDate(moment('1234-12-23'), 'YYYYMMDD');
                });

                it('returns formatted date string', function()
                {
                    expect(this.result).toBe('12341223');
                });
            });

            describe('when JS date is passed', function()
            {
                beforeEach(function()
                {
                    this.result = DateService.formatDate(moment('1234-12-23').toDate(), 'YYYYMMDD');
                });

                it('returns formatted date string', function()
                {
                    expect(this.result).toBe('12341223');
                });
            });

            describe('when null date is passed', function()
            {
                beforeEach(function()
                {
                    this.result = DateService.formatDate(null, 'YYYYMMDD');
                });

                it('returns null', function()
                {
                    expect(this.result).toBeNull();
                });
            });
        });

        describe('parseDate()', function()
        {
            describe('when valid date string is passed', function()
            {
                beforeEach(function()
                {
                    this.result = DateService.parseDate('12341223', 'YYYYMMDD');
                });

                it('returns type of JS date', function()
                {
                    expect(this.result).toEqual(jasmine.any(Date));
                });

                it('returns correct date', function()
                {
                    expect(this.result).toEqual(moment('1234-12-23').toDate());
                });
            });

            describe('when invalid date string is passed', function()
            {
                beforeEach(function()
                {
                    this.result = DateService.parseDate('invalid_string', 'YYYYMMDD');
                });

                it('returns null', function()
                {
                    expect(this.result).toBeNull();
                });
            });
        });
    });
})();