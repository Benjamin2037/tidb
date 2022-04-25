
# default 4 worker.
## ddl on alien; 
MySQL [knull]> create index idx_a on t1m(va);
Query OK, 0 rows affected (28.22 sec)

MySQL [knull]>  create index idx_a on t10m(va);
Query OK, 0 rows affected (2 min 52.12 sec)


## ddl on old;
MySQL [knull]> create index idx_b on t1m(vb);
Query OK, 0 rows affected (32.96 sec)

MySQL [knull]> create index idx_b on t10m(vb);
Query OK, 0 rows affected (4 min 5.87 sec)

# set 8 worker.

245.87
(4 min 5.87 sec)



