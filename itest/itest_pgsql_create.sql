CREATE USER itest;
CREATE DATABASE itest OWNER itest;
GRANT ALL PRIVILEGES ON DATABASE itest TO itest;

\c itest;
CREATE SEQUENCE users_id_sequence;
/* Create test tables */
CREATE TABLE users (
       id int DEFAULT nextval('users_id_sequence') PRIMARY KEY,
       first_name varchar(80),
       last_name varchar(80),
       high_score int
);

GRANT ALL PRIVILEGES ON TABLE users TO itest;
GRANT ALL PRIVILEGES ON SEQUENCE users_id_sequence TO itest;
