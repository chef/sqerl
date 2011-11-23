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

CREATE TABLE `nodes` (
  `id` char(32) NOT NULL,
  `authz_id` char(32) NOT NULL,
  `org_id` char(32) NOT NULL,
  `name` varchar(255) NOT NULL,
  `environment` varchar(255) NOT NULL,
  `serialized_object` blob,
  `last_updated_by` char(32) NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `authz_id` (`authz_id`),
  UNIQUE KEY `org_id` (`org_id`,`name`),
  KEY `nodes_org_id_index` (`org_id`),
  KEY `nodes_org_id_environment_index` (`org_id`,`environment`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
