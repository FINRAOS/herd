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

    describe('dm.utils', function()
    {
        describe('equalsIgnoreCase()', function()
        {
            describe('when str1 === str2', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.equalsIgnoreCase('TEST', 'TEST');
                });

                it('returns true', function()
                {
                    expect(this.result).toBe(true);
                });
            });

            describe('when str1 === str2, but case is different', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.equalsIgnoreCase('TEST', 'test');
                });

                it('returns true', function()
                {
                    expect(this.result).toBe(true);
                });
            });

            describe('when str1 != str2', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.equalsIgnoreCase('TEST', 'MISMATCH');
                });

                it('returns false', function()
                {
                    expect(this.result).toBe(false);
                });
            });

            describe('when str1 === null', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.equalsIgnoreCase(null, 'TEST');
                });

                it('returns false', function()
                {
                    expect(this.result).toBe(false);
                });
            });

            describe('when str2 === null', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.equalsIgnoreCase('TEST', null);
                });

                it('returns false', function()
                {
                    expect(this.result).toBe(false);
                });
            });

            describe('when str1 === null and str2 === null', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.equalsIgnoreCase(null, null);
                });

                it('returns true', function()
                {
                    expect(this.result).toBe(true);
                });
            });
        });

        describe('containsIgnoreCase()', function()
        {
            describe('when str contains searchValue', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.containsIgnoreCase('TEST_CONTAINS_TEST', 'CONTAINS');
                });

                it('returns true', function()
                {
                    expect(this.result).toBe(true);
                });
            });

            describe('when str contains searchValue, but in different case', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.containsIgnoreCase('TEST_CONTAINS_TEST', 'contains');
                });

                it('returns true', function()
                {
                    expect(this.result).toBe(true);
                });
            });

            describe('when str === searchValue', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.containsIgnoreCase('CONTAINS', 'CONTAINS');
                });

                it('returns true', function()
                {
                    expect(this.result).toBe(true);
                });
            });

            describe('when str does not contain searchValue', function()
            {
                beforeEach(function()
                {
                    this.result = dm.utils.containsIgnoreCase('TEST_CONTAINS_TEST', 'NOT_CONTAIN');
                });

                it('returns false', function()
                {
                    expect(this.result).toBe(false);
                });
            });
        });
    });
})();