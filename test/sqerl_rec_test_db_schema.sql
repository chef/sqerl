CREATE DOMAIN kitchen_time AS TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL;

CREATE TABLE kitchens (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE cookers(
  id BIGSERIAL PRIMARY KEY,
  kitchen_id BIGINT NOT NULL REFERENCES kitchens(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  UNIQUE(kitchen_id, name),
  auth_token TEXT NOT NULL UNIQUE,
  auth_token_bday kitchen_time,
  ssh_pub_key TEXT,
  first_name TEXT,
  last_name TEXT,
  email TEXT
);
