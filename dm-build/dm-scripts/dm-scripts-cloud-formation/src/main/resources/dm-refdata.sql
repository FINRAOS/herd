--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

-- SET search_path = dmrowner, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Data for Name: actn_type_cd_lk; Type: TABLE DATA; Schema: dmrowner; Owner: dmrowner
--

INSERT INTO actn_type_cd_lk VALUES ('JOB', 'Job', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


--
-- Data for Name: bus_objct_data_stts_cd_lk; Type: TABLE DATA; Schema: dmrowner; Owner: dmrowner
--

INSERT INTO bus_objct_data_stts_cd_lk VALUES ('EXPIRED', 'Expired', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO bus_objct_data_stts_cd_lk VALUES ('INVALID', 'Invalid', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO bus_objct_data_stts_cd_lk VALUES ('ARCHIVED', 'Archived', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO bus_objct_data_stts_cd_lk VALUES ('VALID', 'Valid', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO bus_objct_data_stts_cd_lk VALUES ('UPLOADING', 'Uploading', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO bus_objct_data_stts_cd_lk VALUES ('RE-ENCRYPTING', 'RE-ENCRYPTING', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO bus_objct_data_stts_cd_lk VALUES ('DELETED', 'Deleted', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

--
-- Data for Name: file_type_cd_lk; Type: TABLE DATA; Schema: dmrowner; Owner: dmrowner
--

INSERT INTO file_type_cd_lk VALUES ('BZ', 'BZIP2 compressed data', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO file_type_cd_lk VALUES ('GZ', 'GNU Zip file', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO file_type_cd_lk VALUES ('ORC', 'Optimized Row Columnar file', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


--
-- Data for Name: ntfcn_event_type_cd_lk; Type: TABLE DATA; Schema: dmrowner; Owner: dmrowner
--

INSERT INTO ntfcn_event_type_cd_lk VALUES ('BUS_OBJCT_DATA_RGSTN', 'Business Object Data Registration', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO ntfcn_event_type_cd_lk VALUES ('BUS_OBJCT_DATA_STTS_CHG', 'Business Object Data Status Change', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


--
-- Data for Name: ntfcn_type_cd_lk; Type: TABLE DATA; Schema: dmrowner; Owner: dmrowner
--

INSERT INTO ntfcn_type_cd_lk VALUES ('BUS_OBJCT_DATA', 'Business Object Data', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


--
-- Data for Name: strge_pltfm; Type: TABLE DATA; Schema: dmrowner; Owner: dmrowner
--

INSERT INTO strge_pltfm VALUES ('GP', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO strge_pltfm VALUES ('HDFS', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO strge_pltfm VALUES ('NZ', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO strge_pltfm VALUES ('ORACLE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');
INSERT INTO strge_pltfm VALUES ('S3', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');


--
-- Data for Name: data_prvdr; Type: TABLE DATA; Schema: dmrowner; Owner: dmrowner
--
INSERT INTO data_prvdr VALUES ('EXCHANGE', current_timestamp, 'SYSTEM', current_timestamp, 'SYSTEM');

--
-- PostgreSQL database dump complete
--

