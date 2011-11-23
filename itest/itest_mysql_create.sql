create user 'itest'@'localhost' identified by 'itest';
create database itest;
grant all on itest.* to 'itest'@'localhost';

use itest;
/* Create test tables */
create table users (
       id int not null auto_increment,
       first_name varchar(80),
       last_name varchar(80),
       high_score int,
       created timestamp,
       datablob blob,
       primary key (id)
) engine=InnoDB;
