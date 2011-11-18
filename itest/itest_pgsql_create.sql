CREATE USER itest;
CREATE DATABASE itest OWNER itest;

\c itest;
CREATE SEQUENCE users_id_sequence
/* Create test tables */
CREATE TABLE users (
       id int DEFAULT nextval('users_id_sequence') PRIMARY KEY,
       first_name varchar(80),
       last_name varchar(80)
);
