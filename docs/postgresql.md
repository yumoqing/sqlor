# POSTGRESQL 

## create database
create database testdb;

## create user 

create role guest with login;

## grat privileges

grant all privileges on database testdb to guest;

### grant table privileges

psql -d testdb

grant all privileges on all tables in schema public to guest;


