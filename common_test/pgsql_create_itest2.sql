-- Multipool testing - ensuring that our connections
-- aren't mixed up.
CREATE TABLE only_in_itest_sqerl_db ( name VARCHAR(256 );
GRANT ALL PRIVILEGES ON TABLE only_in_itest_sqerl_db TO itest_sqerl;

CREATE USER itest_sqerl_pool2 WITH ENCRYPTED PASSWORD 'itest_sqerl_pool2';
CREATE DATABASE itest2 OWNER itest2;
GRANT ALL PRIVILEGES ON DATABASE itest TO itest2;
CREATE TABLE only_in_itest2_db ( name VARCHAR(256 );
GRANT ALL PRIVILEGES ON TABLE only_in_itest_pool2_db TO 'itest_sqerl_pool2';
