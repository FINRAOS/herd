-------------------------------------------------------------------------
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
-------------------------------------------------------------------------

1) Create DM tables
	- ./dm-0.1.0/dm-0.1.0-postgres.create.sql

Create Quartz tables. Note that this file is distributed by Quartz (v2.2.1) and is included out of convenience
	- ./dm-0.1.0/quartz-2.2.1/quartz-2.2.1-postgres.create.sql

3) Create Activiti tables. 
	- Currently support Activiti version 5.16.3.0

4) Insert reference data
	- ./dm-0.1.0/dm-0.1.0-postgres.refdata.sql
	
5) Configure environment
	- Please read the configuration page: https://github.com/FINRAOS/herd/wiki/configuration-values