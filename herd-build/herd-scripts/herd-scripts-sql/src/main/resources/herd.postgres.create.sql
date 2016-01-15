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

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET default_tablespace = '';

SET default_with_oids = false;

CREATE TABLE actn_type_cd_lk (
    actn_type_cd character varying(20) NOT NULL,
    actn_type_ds character varying(50) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE app_endpoint_lk (
    scrty_fn_cd character varying(100) NOT NULL,
    uri character varying(1000) NOT NULL,
    rqst_mthd character varying(20) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE bus_objct_data (
    bus_objct_data_id bigint NOT NULL,
    bus_objct_frmt_id bigint NOT NULL,
    vrsn_nb bigint NOT NULL,
    ltst_vrsn_fl character(1) DEFAULT 'N'::bpchar NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    prtn_value_tx character varying(50) NOT NULL,
    prtn_value_2_tx character varying(30),
    prtn_value_3_tx character varying(30),
    prtn_value_4_tx character varying(30),
    prtn_value_5_tx character varying(30),
    bus_objct_data_stts_cd character varying(20) NOT NULL,
    CONSTRAINT bus_objct_data_ck1 CHECK ((ltst_vrsn_fl = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])))
);


CREATE TABLE bus_objct_dfntn (
    bus_objct_dfntn_id bigint NOT NULL,
    name_tx character varying(50) NOT NULL,
    desc_tx character varying(500),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    data_prvdr_cd character varying(25) NOT NULL,
    name_space_cd character varying(25) NOT NULL,
    lgcy_fl character(1)
);


CREATE TABLE bus_objct_frmt (
    bus_objct_frmt_id bigint NOT NULL,
    bus_objct_dfntn_id bigint NOT NULL,
    usage_cd character varying(20) NOT NULL,
    file_type_cd character varying(20) NOT NULL,
    frmt_vrsn_nb bigint NOT NULL,
    ltst_vrsn_fl character(1) DEFAULT 'N'::bpchar NOT NULL,
    desc_tx character varying(100),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    prtn_key_tx character varying(30) NOT NULL,
    null_value_tx character varying(4),
    dlmtr_tx character varying(4),
    escp_char_tx character varying(4),
    prtn_key_group_tx character varying(30),
    CONSTRAINT bus_objct_frmt_ck1 CHECK ((ltst_vrsn_fl = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])))
);


CREATE TABLE strge_file (
    strge_file_id bigint NOT NULL,
    strge_unit_id bigint NOT NULL,
    fully_qlfd_file_nm character varying(1024) NOT NULL,
    file_size_in_bytes_nb bigint,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    row_ct bigint
);


CREATE TABLE strge_unit (
    strge_unit_id bigint NOT NULL,
    bus_objct_data_id bigint NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    strge_cd character varying(25) NOT NULL,
    drcty_path_tx character varying(1024)
);

CREATE VIEW biz_dt_file_vw AS
 SELECT sf.strge_file_id,
    df.name_space_cd AS namespace,
    df.name_tx AS bus_object_name,
    df.data_prvdr_cd AS data_provider,
    su.strge_cd AS storage_code,
    sf.file_size_in_bytes_nb,
    (sf.creat_ts)::date AS creat_td
   FROM ((((strge_file sf
     JOIN strge_unit su ON ((sf.strge_unit_id = su.strge_unit_id)))
     JOIN bus_objct_data d ON ((su.bus_objct_data_id = d.bus_objct_data_id)))
     JOIN bus_objct_frmt f ON ((d.bus_objct_frmt_id = f.bus_objct_frmt_id)))
     JOIN bus_objct_dfntn df ON ((f.bus_objct_dfntn_id = df.bus_objct_dfntn_id)));


CREATE TABLE bus_objct_data_atrbt (
    bus_objct_data_atrbt_id bigint NOT NULL,
    bus_objct_data_id bigint NOT NULL,
    atrbt_nm character varying(100) NOT NULL,
    atrbt_value_tx character varying(4000),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE bus_objct_data_atrbt_dfntn (
    bus_objct_data_atrbt_dfntn_id bigint NOT NULL,
    bus_objct_frmt_id bigint NOT NULL,
    atrbt_nm character varying(100) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE bus_objct_data_atrbt_dfntn_seq
    START WITH 33617
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;

CREATE SEQUENCE bus_objct_data_atrbt_seq
    START WITH 837956
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE bus_objct_data_prnt (
    bus_objct_data_id bigint NOT NULL,
    prnt_bus_objct_data_id bigint NOT NULL
);

CREATE SEQUENCE bus_objct_data_seq
    START WITH 390953
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE bus_objct_data_stts_cd_lk (
    bus_objct_data_stts_cd character varying(20) NOT NULL,
    bus_objct_data_stts_ds character varying(50) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE bus_objct_data_stts_hs (
    bus_objct_data_stts_hs_id bigint NOT NULL,
    bus_objct_data_id bigint NOT NULL,
    bus_objct_data_stts_cd character varying(20) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE bus_objct_data_stts_hs_seq
    START WITH 367921
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE bus_objct_dfntn_atrbt (
    bus_objct_dfntn_atrbt_id bigint NOT NULL,
    bus_objct_dfntn_id bigint NOT NULL,
    atrbt_nm character varying(100) NOT NULL,
    atrbt_value_tx character varying(4000),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE bus_objct_dfntn_atrbt_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;

CREATE SEQUENCE bus_objct_dfntn_seq
    START WITH 60017
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;

CREATE SEQUENCE bus_objct_frmt_seq
    START WITH 69614
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE cnfgn (
    cnfgn_key_nm character varying(100) NOT NULL,
    cnfgn_value_ds character varying(4000),
    cnfgn_value_cl text
);


CREATE TABLE cstm_ddl (
    cstm_ddl_id bigint NOT NULL,
    bus_objct_frmt_id bigint NOT NULL,
    name_tx character varying(24) NOT NULL,
    ddl_cl text NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE cstm_ddl_seq
    START WITH 521
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE data_prvdr (
    data_prvdr_cd character varying(25) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE db_user (
    user_id character varying(30) NOT NULL,
    last_nm character varying(30) NOT NULL,
    first_nm character varying(30) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL
);


CREATE TABLE deply_jrnl (
    deply_jrnl_id bigint NOT NULL,
    rls_nb character varying(20) NOT NULL,
    rls_stts_cd character varying(20) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    cmpnt_cd character varying(20) NOT NULL
);

CREATE SEQUENCE deply_jrnl_seq
    START WITH 784
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE ec2_od_prcng_lk (
    ec2_od_prcng_id bigint NOT NULL,
    rgn_nm character varying(25) NOT NULL,
    instc_type character varying(25) NOT NULL,
    hrly_pr numeric(7,5) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE ec2_od_prcng_seq
    START WITH 461
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE emr_clstr_crtn_log (
    emr_clstr_crtn_log_id bigint NOT NULL,
    name_space_cd character varying(25) NOT NULL,
    emr_clstr_dfntn_name_tx character varying(25) NOT NULL,
    emr_clstr_id character varying(20) NOT NULL,
    emr_clstr_name_tx character varying(100) NOT NULL,
    emr_clstr_dfntn_cl text NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE emr_clstr_crtn_log_seq
    START WITH 11521
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE emr_clstr_dfntn (
    emr_clstr_dfntn_id bigint NOT NULL,
    name_space_cd character varying(25) NOT NULL,
    name_tx character varying(25) NOT NULL,
    cnfgn_cl text NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE emr_clstr_dfntn_seq
    START WITH 2113
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE file_type_cd_lk (
    file_type_cd character varying(20) NOT NULL,
    file_type_ds character varying(50),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE jms_msg (
    jms_msg_id bigint NOT NULL,
    jms_queue_nm character varying(100) NOT NULL,
    msg_tx text NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE jms_msg_seq
    START WITH 218881
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE job_dfntn (
    job_dfntn_id bigint NOT NULL,
    name_space_cd character varying(25) NOT NULL,
    name_tx character varying(100) NOT NULL,
    desc_tx character varying(500),
    activiti_id character varying(64) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    s3_prpty_buckt_nm character varying(500),
    s3_prpty_objct_key character varying(500)
);


CREATE TABLE job_dfntn_atrbt (
    job_dfntn_atrbt_id bigint NOT NULL,
    job_dfntn_id bigint NOT NULL,
    atrbt_nm character varying(100) NOT NULL,
    atrbt_value_tx text,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE job_dfntn_atrbt_seq
    START WITH 987
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;

CREATE SEQUENCE job_dfntn_seq
    START WITH 1944
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE name_space (
    name_space_cd character varying(25) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE ntfcn_actn (
    ntfcn_actn_id bigint NOT NULL,
    ntfcn_rgstn_id bigint NOT NULL,
    actn_type_cd character varying(20) NOT NULL,
    job_dfntn_id bigint,
    crltn_data_tx text,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE ntfcn_actn_seq
    START WITH 101
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE ntfcn_event_type_cd_lk (
    ntfcn_event_type_cd character varying(50) NOT NULL,
    ntfcn_event_type_ds character varying(50) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE ntfcn_rgstn (
    ntfcn_rgstn_id bigint NOT NULL,
    name_space_cd character varying(25) NOT NULL,
    name_tx character varying(200) NOT NULL,
    ntfcn_type_cd character varying(20) NOT NULL,
    ntfcn_event_type_cd character varying(50) NOT NULL,
    bus_objct_dfntn_id bigint,
    usage_cd character varying(20),
    file_type_cd character varying(20),
    frmt_vrsn_nb bigint,
    strge_cd character varying(25),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE ntfcn_rgstn_seq
    START WITH 81
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE ntfcn_type_cd_lk (
    ntfcn_type_cd character varying(20) NOT NULL,
    ntfcn_type_ds character varying(50) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE prtn_key_group (
    prtn_key_group_tx character varying(30) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE schm_clmn (
    schm_clmn_id bigint NOT NULL,
    bus_objct_frmt_id bigint NOT NULL,
    clmn_name_tx character varying(100) NOT NULL,
    clmn_type_cd character varying(40) NOT NULL,
    clmn_size_tx character varying(10),
    clmn_rqrd_fl character(1),
    clmn_dflt_tx character varying(50),
    clmn_pstn_nb bigint,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    clmn_ds character varying(100),
    prtn_level_nb bigint,
    CONSTRAINT schm_clmn_ck1 CHECK ((clmn_rqrd_fl = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])))
);

CREATE SEQUENCE schm_clmn_seq
    START WITH 5609527
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE scrty_fn_lk (
    scrty_fn_cd character varying(100) NOT NULL,
    scrty_fn_dsply_nm character varying(200),
    scrty_fn_ds character varying(500),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE scrty_role (
    scrty_role_cd character varying(100) NOT NULL,
    scrty_role_ds character varying(500),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);


CREATE TABLE scrty_role_fn (
    scrty_role_fn_id bigint NOT NULL,
    scrty_role_cd character varying(100) NOT NULL,
    scrty_fn_cd character varying(100) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE scrty_role_fn_seq
    START WITH 281
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE strge (
    strge_cd character varying(25) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100),
    strge_pltfm_cd character varying(25) NOT NULL
);


CREATE TABLE strge_atrbt (
    strge_atrbt_id bigint NOT NULL,
    strge_cd character varying(25) NOT NULL,
    atrbt_nm character varying(100) NOT NULL,
    atrbt_value_tx character varying(4000),
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE strge_atrbt_seq
    START WITH 8621
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;

CREATE SEQUENCE strge_file_seq
    START WITH 9482471
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE strge_pltfm (
    strge_pltfm_cd character varying(25) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE strge_unit_seq
    START WITH 392488
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


CREATE TABLE xpctd_prtn_value (
    xpctd_prtn_value_id bigint NOT NULL,
    prtn_key_group_tx character varying(30) NOT NULL,
    prtn_value_tx character varying(30) NOT NULL,
    creat_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    creat_user_id character varying(100) NOT NULL,
    updt_ts timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone NOT NULL,
    updt_user_id character varying(100)
);

CREATE SEQUENCE xpctd_prtn_value_seq
    START WITH 143258
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 20;


ALTER TABLE ONLY actn_type_cd_lk
    ADD CONSTRAINT actn_type_cd_lk_pk PRIMARY KEY (actn_type_cd);


ALTER TABLE ONLY app_endpoint_lk
    ADD CONSTRAINT app_endpoint_pk PRIMARY KEY (scrty_fn_cd, rqst_mthd, uri);


ALTER TABLE ONLY bus_objct_data_atrbt_dfntn
    ADD CONSTRAINT bus_objct_data_atrbt_dfntn_pk PRIMARY KEY (bus_objct_data_atrbt_dfntn_id);


ALTER TABLE ONLY bus_objct_data_atrbt
    ADD CONSTRAINT bus_objct_data_atrbt_pk PRIMARY KEY (bus_objct_data_atrbt_id);


ALTER TABLE ONLY bus_objct_data
    ADD CONSTRAINT bus_objct_data_pk PRIMARY KEY (bus_objct_data_id);


ALTER TABLE ONLY bus_objct_data_prnt
    ADD CONSTRAINT bus_objct_data_prnt_pk PRIMARY KEY (bus_objct_data_id, prnt_bus_objct_data_id);


ALTER TABLE ONLY bus_objct_data_stts_cd_lk
    ADD CONSTRAINT bus_objct_data_stts_cd_lk_pk PRIMARY KEY (bus_objct_data_stts_cd);


ALTER TABLE ONLY bus_objct_data_stts_hs
    ADD CONSTRAINT bus_objct_data_stts_hs_pk PRIMARY KEY (bus_objct_data_stts_hs_id);


ALTER TABLE ONLY bus_objct_dfntn_atrbt
    ADD CONSTRAINT bus_objct_dfntn_atrbt_pk PRIMARY KEY (bus_objct_dfntn_atrbt_id);


ALTER TABLE ONLY bus_objct_dfntn
    ADD CONSTRAINT bus_objct_dfntn_pk PRIMARY KEY (bus_objct_dfntn_id);


ALTER TABLE ONLY bus_objct_frmt
    ADD CONSTRAINT bus_objct_frmt_pk PRIMARY KEY (bus_objct_frmt_id);


ALTER TABLE ONLY cnfgn
    ADD CONSTRAINT cnfgn_pk PRIMARY KEY (cnfgn_key_nm);


ALTER TABLE ONLY cstm_ddl
    ADD CONSTRAINT cstm_ddl_pk PRIMARY KEY (cstm_ddl_id);


ALTER TABLE ONLY data_prvdr
    ADD CONSTRAINT data_prvdr_pk PRIMARY KEY (data_prvdr_cd);


ALTER TABLE ONLY db_user
    ADD CONSTRAINT db_user_pk PRIMARY KEY (user_id);


ALTER TABLE ONLY deply_jrnl
    ADD CONSTRAINT deply_jrnl_pk PRIMARY KEY (deply_jrnl_id);


ALTER TABLE ONLY ec2_od_prcng_lk
    ADD CONSTRAINT ec2_od_prcng_pk PRIMARY KEY (ec2_od_prcng_id);


ALTER TABLE ONLY emr_clstr_crtn_log
    ADD CONSTRAINT emr_clstr_crtn_log_pk PRIMARY KEY (emr_clstr_crtn_log_id);


ALTER TABLE ONLY emr_clstr_dfntn
    ADD CONSTRAINT emr_clstr_dfntn_pk PRIMARY KEY (emr_clstr_dfntn_id);


ALTER TABLE ONLY file_type_cd_lk
    ADD CONSTRAINT file_type_cd_lk_pk PRIMARY KEY (file_type_cd);


ALTER TABLE ONLY jms_msg
    ADD CONSTRAINT jms_msg_pk PRIMARY KEY (jms_msg_id);


ALTER TABLE ONLY job_dfntn_atrbt
    ADD CONSTRAINT job_dfntn_atrbt_pk PRIMARY KEY (job_dfntn_atrbt_id);


ALTER TABLE ONLY job_dfntn
    ADD CONSTRAINT job_dfntn_pk PRIMARY KEY (job_dfntn_id);


ALTER TABLE ONLY name_space
    ADD CONSTRAINT name_space_pk PRIMARY KEY (name_space_cd);


ALTER TABLE ONLY ntfcn_actn
    ADD CONSTRAINT ntfcn_actn_pk PRIMARY KEY (ntfcn_actn_id);


ALTER TABLE ONLY ntfcn_event_type_cd_lk
    ADD CONSTRAINT ntfcn_event_type_cd_lk_pk PRIMARY KEY (ntfcn_event_type_cd);


ALTER TABLE ONLY ntfcn_rgstn
    ADD CONSTRAINT ntfcn_rgstn_pk PRIMARY KEY (ntfcn_rgstn_id);


ALTER TABLE ONLY ntfcn_type_cd_lk
    ADD CONSTRAINT ntfcn_type_cd_lk_pk PRIMARY KEY (ntfcn_type_cd);


ALTER TABLE ONLY prtn_key_group
    ADD CONSTRAINT prtn_key_group_pk PRIMARY KEY (prtn_key_group_tx);


ALTER TABLE ONLY schm_clmn
    ADD CONSTRAINT schm_clmn_pk PRIMARY KEY (schm_clmn_id);


ALTER TABLE ONLY scrty_fn_lk
    ADD CONSTRAINT scrty_fn_pk PRIMARY KEY (scrty_fn_cd);


ALTER TABLE ONLY scrty_role_fn
    ADD CONSTRAINT scrty_role_fn_pk PRIMARY KEY (scrty_role_fn_id);


ALTER TABLE ONLY scrty_role
    ADD CONSTRAINT scrty_role_pk PRIMARY KEY (scrty_role_cd);


ALTER TABLE ONLY strge_atrbt
    ADD CONSTRAINT strge_atrbt_pk PRIMARY KEY (strge_atrbt_id);


ALTER TABLE ONLY strge_file
    ADD CONSTRAINT strge_file_pk PRIMARY KEY (strge_file_id);


ALTER TABLE ONLY strge
    ADD CONSTRAINT strge_pk PRIMARY KEY (strge_cd);


ALTER TABLE ONLY strge_pltfm
    ADD CONSTRAINT strge_pltfm_pk PRIMARY KEY (strge_pltfm_cd);


ALTER TABLE ONLY strge_unit
    ADD CONSTRAINT strge_unit_pk PRIMARY KEY (strge_unit_id);


ALTER TABLE ONLY xpctd_prtn_value
    ADD CONSTRAINT xpctd_prtn_value_pk PRIMARY KEY (xpctd_prtn_value_id);


CREATE UNIQUE INDEX bus_objct_data_ak ON bus_objct_data USING btree (bus_objct_frmt_id, prtn_value_tx, prtn_value_2_tx, prtn_value_3_tx, prtn_value_4_tx, prtn_value_5_tx, vrsn_nb);


CREATE UNIQUE INDEX bus_objct_data_atrbt_ak ON bus_objct_data_atrbt USING btree (bus_objct_data_id, atrbt_nm);


CREATE UNIQUE INDEX bus_objct_data_atrbt_dfntn_ak ON bus_objct_data_atrbt_dfntn USING btree (bus_objct_frmt_id, atrbt_nm);


CREATE INDEX bus_objct_data_atrbt_dfntn_ix1 ON bus_objct_data_atrbt_dfntn USING btree (bus_objct_frmt_id);


CREATE INDEX bus_objct_data_atrbt_ix1 ON bus_objct_data_atrbt USING btree (bus_objct_data_id);


CREATE INDEX bus_objct_data_ix1 ON bus_objct_data USING btree (bus_objct_frmt_id);


CREATE INDEX bus_objct_data_ix2 ON bus_objct_data USING btree (bus_objct_data_stts_cd);


CREATE INDEX bus_objct_data_prnt_ix1 ON bus_objct_data_prnt USING btree (bus_objct_data_id);


CREATE INDEX bus_objct_data_prnt_ix2 ON bus_objct_data_prnt USING btree (prnt_bus_objct_data_id);


CREATE UNIQUE INDEX bus_objct_data_stts_hs_ak ON bus_objct_data_stts_hs USING btree (bus_objct_data_id, bus_objct_data_stts_cd, creat_ts);


CREATE INDEX bus_objct_data_stts_hs_ix1 ON bus_objct_data_stts_hs USING btree (bus_objct_data_id);


CREATE INDEX bus_objct_data_stts_hs_ix2 ON bus_objct_data_stts_hs USING btree (bus_objct_data_stts_cd);


CREATE UNIQUE INDEX bus_objct_dfntn_ak ON bus_objct_dfntn USING btree (name_space_cd, name_tx);


CREATE UNIQUE INDEX bus_objct_dfntn_atrbt_ak ON bus_objct_dfntn_atrbt USING btree (bus_objct_dfntn_id, atrbt_nm);


CREATE INDEX bus_objct_dfntn_ix1 ON bus_objct_dfntn USING btree (data_prvdr_cd);


CREATE INDEX bus_objct_dfntn_ix2 ON bus_objct_dfntn USING btree (name_space_cd);


CREATE UNIQUE INDEX bus_objct_frmt_ak ON bus_objct_frmt USING btree (bus_objct_dfntn_id, usage_cd, file_type_cd, frmt_vrsn_nb);


CREATE INDEX bus_objct_frmt_ix1 ON bus_objct_frmt USING btree (bus_objct_dfntn_id);


CREATE INDEX bus_objct_frmt_ix2 ON bus_objct_frmt USING btree (file_type_cd);


CREATE INDEX bus_objct_frmt_ix3 ON bus_objct_frmt USING btree (prtn_key_group_tx);


CREATE UNIQUE INDEX cstm_ddl_ak ON cstm_ddl USING btree (bus_objct_frmt_id, name_tx);


CREATE INDEX cstm_ddl_ix1 ON cstm_ddl USING btree (bus_objct_frmt_id);


CREATE UNIQUE INDEX ec2_od_prcng_ak ON ec2_od_prcng_lk USING btree (rgn_nm, instc_type);


CREATE UNIQUE INDEX emr_clstr_crtn_log_ak ON emr_clstr_crtn_log USING btree (name_space_cd, emr_clstr_dfntn_name_tx, emr_clstr_id);


CREATE INDEX emr_clstr_crtn_log_ix1 ON emr_clstr_crtn_log USING btree (name_space_cd);


CREATE UNIQUE INDEX emr_clstr_dfntn_ak ON emr_clstr_dfntn USING btree (name_space_cd, name_tx);


CREATE INDEX emr_clstr_dfntn_ix1 ON emr_clstr_dfntn USING btree (name_space_cd);


CREATE UNIQUE INDEX job_dfntn_ak ON job_dfntn USING btree (name_space_cd, name_tx);


CREATE UNIQUE INDEX job_dfntn_atrbt_ak ON job_dfntn_atrbt USING btree (job_dfntn_id, atrbt_nm);


CREATE INDEX job_dfntn_atrbt_ix1 ON job_dfntn_atrbt USING btree (job_dfntn_id);


CREATE INDEX job_dfntn_ix1 ON job_dfntn USING btree (name_space_cd);


CREATE UNIQUE INDEX job_dfntn_ix2 ON job_dfntn USING btree (activiti_id);


CREATE UNIQUE INDEX ntfcn_actn_ak ON ntfcn_actn USING btree (ntfcn_rgstn_id, actn_type_cd, job_dfntn_id);


CREATE INDEX ntfcn_actn_ix1 ON ntfcn_actn USING btree (ntfcn_rgstn_id);


CREATE INDEX ntfcn_actn_ix2 ON ntfcn_actn USING btree (job_dfntn_id);


CREATE UNIQUE INDEX ntfcn_rgstn_ak ON ntfcn_rgstn USING btree (name_space_cd, name_tx, ntfcn_type_cd);


CREATE INDEX ntfcn_rgstn_ix1 ON ntfcn_rgstn USING btree (name_space_cd);


CREATE INDEX ntfcn_rgstn_ix2 ON ntfcn_rgstn USING btree (ntfcn_type_cd);


CREATE INDEX ntfcn_rgstn_ix3 ON ntfcn_rgstn USING btree (ntfcn_event_type_cd);


CREATE INDEX ntfcn_rgstn_ix4 ON ntfcn_rgstn USING btree (bus_objct_dfntn_id);


CREATE INDEX ntfcn_rgstn_ix5 ON ntfcn_rgstn USING btree (file_type_cd);


CREATE INDEX ntfcn_rgstn_ix6 ON ntfcn_rgstn USING btree (strge_cd);


CREATE UNIQUE INDEX schm_clmn_ak ON schm_clmn USING btree (bus_objct_frmt_id, clmn_name_tx);


CREATE INDEX schm_clmn_ix1 ON schm_clmn USING btree (bus_objct_frmt_id);


CREATE UNIQUE INDEX scrty_role_fn_ak ON scrty_role_fn USING btree (scrty_role_cd, scrty_fn_cd);


CREATE INDEX scrty_role_fn_ix1 ON scrty_role_fn USING btree (scrty_fn_cd);


CREATE INDEX stge_file_ix1 ON strge_file USING btree (fully_qlfd_file_nm);


CREATE UNIQUE INDEX strge_atrbt_ak ON strge_atrbt USING btree (strge_cd, atrbt_nm);


CREATE INDEX strge_atrbt_ix1 ON strge_atrbt USING btree (strge_cd);


CREATE UNIQUE INDEX strge_file_ak ON strge_file USING btree (strge_unit_id, fully_qlfd_file_nm);


CREATE INDEX strge_ix1 ON strge USING btree (strge_pltfm_cd);


CREATE UNIQUE INDEX strge_unit_ak ON strge_unit USING btree (strge_cd, bus_objct_data_id);


CREATE INDEX strge_unit_ix2 ON strge_unit USING btree (bus_objct_data_id);


CREATE UNIQUE INDEX xpctd_prtn_value_ak ON xpctd_prtn_value USING btree (prtn_key_group_tx, prtn_value_tx);


CREATE INDEX xpctd_prtn_value_ix1 ON xpctd_prtn_value USING btree (prtn_key_group_tx);

ALTER TABLE ONLY app_endpoint_lk
    ADD CONSTRAINT app_endpoint_fk1 FOREIGN KEY (scrty_fn_cd) REFERENCES scrty_fn_lk(scrty_fn_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data_atrbt_dfntn
    ADD CONSTRAINT bus_objct_data_atrbt_dfntn_fk1 FOREIGN KEY (bus_objct_frmt_id) REFERENCES bus_objct_frmt(bus_objct_frmt_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data_atrbt
    ADD CONSTRAINT bus_objct_data_atrbt_fk1 FOREIGN KEY (bus_objct_data_id) REFERENCES bus_objct_data(bus_objct_data_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data
    ADD CONSTRAINT bus_objct_data_fk1 FOREIGN KEY (bus_objct_frmt_id) REFERENCES bus_objct_frmt(bus_objct_frmt_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data
    ADD CONSTRAINT bus_objct_data_fk2 FOREIGN KEY (bus_objct_data_stts_cd) REFERENCES bus_objct_data_stts_cd_lk(bus_objct_data_stts_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data_prnt
    ADD CONSTRAINT bus_objct_data_prnt_fk1 FOREIGN KEY (bus_objct_data_id) REFERENCES bus_objct_data(bus_objct_data_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data_prnt
    ADD CONSTRAINT bus_objct_data_prnt_fk2 FOREIGN KEY (prnt_bus_objct_data_id) REFERENCES bus_objct_data(bus_objct_data_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data_stts_hs
    ADD CONSTRAINT bus_objct_data_stts_hs_fk1 FOREIGN KEY (bus_objct_data_id) REFERENCES bus_objct_data(bus_objct_data_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_data_stts_hs
    ADD CONSTRAINT bus_objct_data_stts_hs_fk2 FOREIGN KEY (bus_objct_data_stts_cd) REFERENCES bus_objct_data_stts_cd_lk(bus_objct_data_stts_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_dfntn_atrbt
    ADD CONSTRAINT bus_objct_dfntn_atrbt_fk1 FOREIGN KEY (bus_objct_dfntn_id) REFERENCES bus_objct_dfntn(bus_objct_dfntn_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_dfntn
    ADD CONSTRAINT bus_objct_dfntn_fk1 FOREIGN KEY (data_prvdr_cd) REFERENCES data_prvdr(data_prvdr_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_dfntn
    ADD CONSTRAINT bus_objct_dfntn_fk2 FOREIGN KEY (name_space_cd) REFERENCES name_space(name_space_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_frmt
    ADD CONSTRAINT bus_objct_frmt_fk1 FOREIGN KEY (bus_objct_dfntn_id) REFERENCES bus_objct_dfntn(bus_objct_dfntn_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_frmt
    ADD CONSTRAINT bus_objct_frmt_fk2 FOREIGN KEY (file_type_cd) REFERENCES file_type_cd_lk(file_type_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY bus_objct_frmt
    ADD CONSTRAINT bus_objct_frmt_fk3 FOREIGN KEY (prtn_key_group_tx) REFERENCES prtn_key_group(prtn_key_group_tx) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY cstm_ddl
    ADD CONSTRAINT cstm_ddl_fk1 FOREIGN KEY (bus_objct_frmt_id) REFERENCES bus_objct_frmt(bus_objct_frmt_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY emr_clstr_crtn_log
    ADD CONSTRAINT emr_clstr_crtn_log_fk1 FOREIGN KEY (name_space_cd) REFERENCES name_space(name_space_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY emr_clstr_dfntn
    ADD CONSTRAINT emr_clstr_dfntn_fk1 FOREIGN KEY (name_space_cd) REFERENCES name_space(name_space_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY job_dfntn_atrbt
    ADD CONSTRAINT job_dfntn_atrbt_fk1 FOREIGN KEY (job_dfntn_id) REFERENCES job_dfntn(job_dfntn_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY job_dfntn
    ADD CONSTRAINT job_dfntn_fk1 FOREIGN KEY (name_space_cd) REFERENCES name_space(name_space_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_actn
    ADD CONSTRAINT ntfcn_actn_fk1 FOREIGN KEY (ntfcn_rgstn_id) REFERENCES ntfcn_rgstn(ntfcn_rgstn_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_actn
    ADD CONSTRAINT ntfcn_actn_fk2 FOREIGN KEY (job_dfntn_id) REFERENCES job_dfntn(job_dfntn_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_rgstn
    ADD CONSTRAINT ntfcn_rgstn_fk1 FOREIGN KEY (name_space_cd) REFERENCES name_space(name_space_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_rgstn
    ADD CONSTRAINT ntfcn_rgstn_fk2 FOREIGN KEY (ntfcn_type_cd) REFERENCES ntfcn_type_cd_lk(ntfcn_type_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_rgstn
    ADD CONSTRAINT ntfcn_rgstn_fk3 FOREIGN KEY (ntfcn_event_type_cd) REFERENCES ntfcn_event_type_cd_lk(ntfcn_event_type_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_rgstn
    ADD CONSTRAINT ntfcn_rgstn_fk4 FOREIGN KEY (bus_objct_dfntn_id) REFERENCES bus_objct_dfntn(bus_objct_dfntn_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_rgstn
    ADD CONSTRAINT ntfcn_rgstn_fk5 FOREIGN KEY (file_type_cd) REFERENCES file_type_cd_lk(file_type_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ntfcn_rgstn
    ADD CONSTRAINT ntfcn_rgstn_fk6 FOREIGN KEY (strge_cd) REFERENCES strge(strge_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY schm_clmn
    ADD CONSTRAINT schm_clmn_fk1 FOREIGN KEY (bus_objct_frmt_id) REFERENCES bus_objct_frmt(bus_objct_frmt_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY scrty_role_fn
    ADD CONSTRAINT scrty_role_fn_fk1 FOREIGN KEY (scrty_role_cd) REFERENCES scrty_role(scrty_role_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY scrty_role_fn
    ADD CONSTRAINT scrty_role_fn_fk2 FOREIGN KEY (scrty_fn_cd) REFERENCES scrty_fn_lk(scrty_fn_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY strge_atrbt
    ADD CONSTRAINT strge_atrbt_fk1 FOREIGN KEY (strge_cd) REFERENCES strge(strge_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY strge_file
    ADD CONSTRAINT strge_file_fk1 FOREIGN KEY (strge_unit_id) REFERENCES strge_unit(strge_unit_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY strge
    ADD CONSTRAINT strge_fk1 FOREIGN KEY (strge_pltfm_cd) REFERENCES strge_pltfm(strge_pltfm_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY strge_unit
    ADD CONSTRAINT strge_unit_fk1 FOREIGN KEY (strge_cd) REFERENCES strge(strge_cd) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY strge_unit
    ADD CONSTRAINT strge_unit_fk2 FOREIGN KEY (bus_objct_data_id) REFERENCES bus_objct_data(bus_objct_data_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY xpctd_prtn_value
    ADD CONSTRAINT xpctd_prtn_value_fk1 FOREIGN KEY (prtn_key_group_tx) REFERENCES prtn_key_group(prtn_key_group_tx) DEFERRABLE INITIALLY DEFERRED;


