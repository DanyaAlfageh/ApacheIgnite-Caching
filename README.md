# ApacheIgnite-Caching

* Apache Ignite  is a memory-centric distributed database, caching, and processing platform for transactional, analytical, and streaming workloads delivering in-memory speeds at petabyte scale. To get more information and download Apache Ignite [here](https://ignite.apache.org/)

This is simple example about caching in the Apache Ignite

Before run the example because of I have used postgres as database, you should change the JDBC connection as your needs. 

If you do not want to change, here is the steps:

* Download the postgres
* Change the password via this command `ALTER USER postgres PASSWORD '1234';`
* Create a database named **testdb**
* Connect your database via `postgres=# \c testdb`
* Create a table name PERSONS
```
CREATE TABLE PERSONS(
    id int,
    name TEXT,
    age int,
    salary decimal
);
```
* Then put some data to the database via `insert into values .... `
* Open the project via IDE
* Run the example

