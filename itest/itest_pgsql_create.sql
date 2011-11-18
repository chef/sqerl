CREATE USER itest;
CREATE DATABASE itest OWNER itest;

\c itest;
/* Create test tables */
create table users (
       id int not null,
       first_name varchar(80),
       last_name varchar(80),
       primary key (id)
);
