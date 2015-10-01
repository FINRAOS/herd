<!--
  Copyright 2015 herd contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Guidelines for writing tests

## Test project structure

The directories under *test/* are separated by component type first, then a **Test.js* files that mirrors the source file under test.

The only exception is *test/integration/* where the integration testing between template and controllers are meant to be stored, and would be developed by the team's dedicated QA resources.

## Writing Jasmine behavior driven tests

The tests shall be wrapped in multiple nested *describe()* blocks. The *describe()* blocks shall be named and nested as below:

- Name of object/file under test
	- Name of function under test
		- *Given* condition (optional)
			- *When* condition
				- Any number of *it()* to validate expectations

This is to ensure that the test output is formatted in such a way that it is easy to read, and to make the test more descriptive.

### An example test

> 
	describe('Sample', function() 
	{
		describe('foo()', function() 
		{
			describe('Given value "bar" exists in database', function() 
			{
				database.save('bar');
				describe('When value "baz" is given in parameter', function() 
				{
					beforeEach(function() {
						this.returnValue = Sample.foo('baz');
					});
					it('Returns "bar"', function() {
						expect(this.returnValue).toBe('bar');
					});
				});
			});
		});
	});
> 

## Writing tests for AngularJS

### Use *inject()* and *$injector* to inject dependencies.

> 
	var $window;
	var SampleService;
	beforeEach(inject(function($injector) {
		$window = $injector.get('$window');
		SampleService = $injector.get('SampleService');
	}));
> 

### Avoid using *_* syntax to inject dependencies

> 
	var $window;
	var SampleService;
	beforeEach(inject(function(_$window_, _SampleService_) {
		$window = _$window_;
		SampleService = _SampleService_;
	}));
> 

This is an easy syntax when injecting only a few dependencies, but it can be troublesome when the number of dependencies grow. Readability becomes an issue as the function parameter grows horizontally. For consistency's sake, use the $injector.

### Use *spyOn()* to mock functions of injected dependencies. 

> 
	describe('foo()', function() {
		beforeEach(function() {
			spyOn(SampleService, 'foo').and.returnValue('bar');
		});
	})
> 

### Override *spyOn()* mocks within *describe(When)*

> 
	describe('when parameter is "baz"', function() {
		beforeEach(function() {
			// this is assuming that SampleService.foo has already been spied on
			SampleService.foo.and.returnValue('overridden');
		});
		it('calls SampleService.foo() with "baz"', function() {
			expect(SampleService.foo).toHaveBeenCalledWith('baz')
		})
	})
> 
