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

CREATE TABLE trstng_accnt  
( 
    trstng_accnt_id_nb                  varchar(50)     NOT NULL,
    role_arn                            varchar(255)    NOT NULL,
    creat_ts                            timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    creat_user_id                       varchar(100)    NOT NULL,
    updt_ts                             timestamp       NOT NULL DEFAULT ('now'::text)::timestamp without time zone,
    updt_user_id                        varchar(100)
);
    
ALTER TABLE trstng_accnt ADD CONSTRAINT trstng_accnt_pk   PRIMARY KEY(trstng_accnt_id_nb);