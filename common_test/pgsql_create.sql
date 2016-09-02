CREATE USER itest_sqerl1 WITH ENCRYPTED PASSWORD 'itest_sqerl1';


CREATE DATABASE itest_sqerl1 OWNER itest_sqerl1;
GRANT ALL PRIVILEGES ON DATABASE itest_sqerl1 TO itest_sqerl1;

\c itest_sqerl1;
CREATE SEQUENCE users_id_sequence;
/* Create test tables */
CREATE TABLE users (
       id int DEFAULT nextval('users_id_sequence') PRIMARY KEY,
       first_name varchar(80),
       last_name varchar(80),
       high_score int,
       active boolean,
       datablob bytea,
       created timestamp
);

GRANT ALL PRIVILEGES ON TABLE users TO itest_sqerl1;
GRANT ALL PRIVILEGES ON SEQUENCE users_id_sequence TO itest_sqerl1;

CREATE TABLE nodes (
       id char(32) PRIMARY KEY,
       authz_id char(32) UNIQUE NOT NULL,
       org_id char(32) NOT NULL,
       name varchar(255) NOT NULL,
       environment varchar(255) NOT NULL,
       serialized_object bytea,
       last_updated_by char(32) NOT NULL,
       created_at timestamp NOT NULL,
       updated_at timestamp NOT NULL
);

GRANT ALL PRIVILEGES ON TABLE nodes TO itest_sqerl1;

CREATE OR REPLACE FUNCTION insert_users(varchar[],
    varchar[],
    int[],
    varchar[],
    boolean[])
RETURNS VOID AS
$$
DECLARE
    i INT4;
BEGIN
    FOR i IN SELECT generate_subscripts( $1, 1 )
    LOOP
        INSERT INTO users
            (first_name, last_name, high_score, created, active)
        VALUES
            ($1[i], $2[i], $3[i], cast($4[i] AS TIMESTAMP), $5[i]);
    END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE TABLE uuids (
       id uuid UNIQUE NOT NULL
);

GRANT ALL PRIVILEGES ON TABLE uuids TO itest_sqerl1;

CREATE OR REPLACE FUNCTION insert_ids(uuid[])
RETURNS VOID AS
$$
DECLARE
    i INT4;
BEGIN
    FOR i IN SELECT generate_subscripts( $1, 1 )
    LOOP
        INSERT INTO uuids (id) VALUES ($1[i]);
    END LOOP;
END;
$$
LANGUAGE plpgsql;

