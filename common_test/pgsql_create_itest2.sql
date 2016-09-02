-- Multipool testing - ensuring that our connections
-- aren't mixed up.
CREATE TABLE only_in_itest_sqerl1_db ( name VARCHAR(256 );
GRANT ALL PRIVILEGES ON TABLE only_in_itest_sqerl1_db TO itest_sqerl1;

CREATE USER itest_sqerl2 WITH ENCRYPTED PASSWORD 'itest_sqerl2';
CREATE DATABASE itest_sqerl2 OWNER itest_sqerl2;
GRANT ALL PRIVILEGES ON DATABASE itest_sqerl2 TO itest_sqerl2;
CREATE TABLE only_in_itest2_db ( name VARCHAR(256 );
GRANT ALL PRIVILEGES ON TABLE only_in_itest_pool2_db TO 'itest_sqerl2';
