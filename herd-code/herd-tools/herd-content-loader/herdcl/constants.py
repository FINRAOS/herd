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
    OBJECTS = 'Descriptive Info'
    SME = 'Contacts'
    OBJECT_TAG = 'Data Entity Tags'
    COLUMNS = 'Columns'
    LINEAGE = 'Lineage'
    SAMPLES = 'Sample Data'
    TAGS = 'Tags'
    RELATIONAL = 'Relational Table'
    LATEST_DESC = 'Latest Descriptive Info'
    LATEST_COLUMNS = 'Latest Columns'
    LATEST_BDEF_TAGS = 'Latest Data Entity Tags'
    ENVS = ['DEV-INT', 'QA-INT', 'CT', 'PROD', 'PRODY-DI', 'PRODY-QI']


class WindowElement(Enum):
    USER = '-User-'
    CRED = '-Cred-'
    ENV = '-Env-'
    SWITCH = '-Switch-'
    ACTION = '-Action-'
    EXPORT_ACTION = '-ExportAction-'
    EXCEL_FILE = '-Excel-'
    SAMPLE_DIR = '-Sample-'
    NAMESPACE = '-Namespace-'
    EXPORT_DIR = '-Export-'
    TEXTPAD = '-Textpad-'
    DEBUG = '-Debug-'


class Summary(Enum):
    TOTAL = 'total_rows'
    SUCCESS = 'success_rows'
    FAIL = 'fail_rows'
    FAIL_INDEX = 'fail_index'
    COMMENTS = 'comments'
    CHANGES = 'changes'
    WARNINGS = 'warnings'
    ERRORS = 'errors'
    INDEX = 'index'
    MESSAGE = 'message'
    TIME = 'start_time'


class Objects(Enum):
    EXCEL_NAME = 'descinfo.xlsx'
    WORKSHEET = 'Descriptive Info'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Data Entity Physical Name'
    FORMAT_USAGE = 'UDC Display Format Usage Type'
    FILE_TYPE = 'UDC Display Format File Type'
    DISPLAY_NAME = 'Business Name'
    DESCRIPTION = 'Description'


class SubjectMatterExpert(Enum):
    WORKSHEET = 'Contacts'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Data Entity Physical Name'
    SME = 'Contacts User ID'


class ObjectTags(Enum):
    EXCEL_NAME = 'dataentitytags.xlsx'
    WORKSHEET = 'Data Entity Tags'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Data Entity Physical Name'


class Samples(Enum):
    WORKSHEET = 'Sample Data'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Data Entity Physical Name'
    SAMPLE = 'Sample Data Files'


class Columns(Enum):
    EXCEL_NAME = 'columns.xlsx'
    WORKSHEET = 'Columns'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Data Entity Physical Name'
    SCHEMA_NAME = 'Physical Name'
    COLUMN_NAME = 'Business Name'
    DESCRIPTION = 'Description'


class Lineage(Enum):
    WORKSHEET = 'Lineage'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Data Entity Physical Name'
    USAGE = 'Format Usage'
    FILE_TYPE = 'Format File Type'


class TagTypes(Enum):
    WORKSHEET = 'Tag Type'
    NAME = 'Tag Type Name'
    CODE = 'Tag Type Code'
    DESCRIPTION = 'Tag Type Description'


class Tags(Enum):
    WORKSHEET = 'Tag'
    NAME = 'Tag Name'
    TAG = 'Tag Type'
    TAGTYPE = 'Tag Type Code'
    DESCRIPTION = 'Tag Desc Text'
    PARENT = 'Parent Tag Name'
    MULTIPLIER = 'Importance'


class Relational(Enum):
    WORKSHEET = 'Relational Table'
    NAMESPACE = 'Namespace'
    DEFINITION_NAME = 'Data Entity Physical Name'
    FORMAT_USAGE = 'Format Usage'
    DATA_PROVIDER_NAME = 'Data Provider Name'
    SCHEMA_NAME = 'Relational Schema Name'
    TABLE_NAME = 'Relational Table Name'
    STORAGE_NAME = 'Storage Name'
    APPEND = 'Append To Existing Data Entity'
