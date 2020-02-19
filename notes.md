
### sqlite_server_test

This tool is designed to run against proxysql, where `SQLite3_Server` is the
backend. In other words `proxysql` needs to be started with `--sqlite3-server`.
Note: Version 2.0.9 or newer is required.

#### ProxySQL configuration

Start proxysql, with `--sqlite3-server`, for example:

```bash
./proxysql -f -D . --sqlite3-server
```

Create users and hostgroup:
```bash
for i in `seq 0 19` ; do
echo "INSERT INTO mysql_users(username,password,default_hostgroup) VALUES (\"user$i\",\"user$i\",100);"
done | mysql -u admin -padmin -h 127.0.0.1 -P6032
for i in `seq 100 119` ; do
echo "INSERT INTO mysql_servers (hostgroup_id, hostname, port) VALUES ($i,'127.0.0.1',6030);"
done | mysql -u admin -padmin -h 127.0.0.1 -P6032
```

Load and save configuration:
```sql
SAVE MYSQL SERVERS TO DISK;
LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;
LOAD MYSQL USERS TO RUNTIME;
```


This tool creates a lot of connections to proxysql, using different credentials,
and using different schemas.
It automatically creates the tables it will ran queries to.
Tables are constantly selected, updated, inserted and deleted.
Transactions are temporary disabled.
In the example below:

```bash
./sqlite_server_test -i 50 -c 100 -u user -p user -U 20 -h 127.0.0.1 -P 6033 -D main -q 3000 -t 4
```

* queries are executed every 50ms (`-i 50`)
* 4 threads are started (`-t 4`)
* each thread opens up to a maximum of 100 connections (`-c 100`). Every 200 (hardcoded) requests, the number of connections is randomly set to within this limit
* user `user` is used as base of usernames (`-u user`)
* password `user` is used as base of passwords (`-p user`)
* 20 users are used (`-U 20`). The users will be from `user0` to `user19`
* it will use by default schema `main` (`-D main`). Although, internally it will randomly switch from schema `main_101` to `main_xxx` , where `xxx` is `101+num_users` (120 in this example)
* each thread will run 3000 queries (`-q 3000`)
