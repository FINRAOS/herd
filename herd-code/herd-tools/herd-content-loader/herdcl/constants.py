"""
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
"""
from enum import Enum

class Menu(Enum):
    OBJECTS = 'Objects'
    COLUMNS = 'Columns'
    LINEAGE = 'Lineage'
    SAMPLES = 'Samples'
    TAGS = 'Tags'
    EXPORT = 'Export BDef'
    ENVS = ['DEV-INT', 'QA-INT', 'CT', 'PROD', 'PROD-CT']

class Summary(Enum):
    CHANGES = 'changes'
    WARNINGS = 'warnings'
    ERRORS = 'errors'

class Objects(Enum):
    WORKSHEET = 'Bus Obj Definition'
    NAMESPACE = 'Bus Obj Def Namespace'
    DEFINITION_NAME = 'Bus Obj Def Name'
    SME = 'Bus Obj Def SME User ID'
    SAMPLE = 'Link to Sample Data'

class Columns(Enum):
    WORKSHEET = 'Business Object Attribute'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Bus Obj Def Name'
    SCHEMA_NAME = 'Bus Obj Attribute Name'
    COLUMN_NAME = 'Business Object Attribute Logical Name'
    DESCRIPTION = 'Business Object Attribute Description'

class Lineage(Enum):
    WORKSHEET = 'Business Object Lineage'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Bus Obj Def Name'
    USAGE = 'Format Usage'
    FILE_TYPE = 'Format File Type'
