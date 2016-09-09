drop database if exists db0;
drop database if exists db1;
create database db0;
create database db1;
use db0;
drop table if exists tb0;
drop table if exists tb1;
create table tb0( order_id INT NOT NULL, product_id INT NOT NULL, usr_id INT NOT NULL, begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, end_time TIMESTAMP, status INT,PRIMARY KEY(order_id));
create table tb1( order_id INT NOT NULL, product_id INT NOT NULL, usr_id INT NOT NULL, begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, end_time TIMESTAMP, status INT,PRIMARY KEY(order_id));

use db1;
drop table if exists tb0;
drop table if exists tb1;
create table tb0( order_id INT NOT NULL, product_id INT NOT NULL, usr_id INT NOT NULL, begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, end_time TIMESTAMP, status INT,PRIMARY KEY(order_id));
create table tb1( order_id INT NOT NULL, product_id INT NOT NULL, usr_id INT NOT NULL, begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, end_time TIMESTAMP, status INT,PRIMARY KEY(order_id));


GRANT ALL ON  db0.* TO 'xujianhai'@'%';
GRANT ALL ON  db1.* TO 'xujianhai'@'%';

flush privileges;

set global general_log=on;
show  variables like 'general_log_file';

# tail -f 日志,在调试情况下使用
