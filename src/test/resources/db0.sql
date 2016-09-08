drop table if exists tb0;
drop table if exists tb1;
create table tb0( order_id INT NOT NULL, product_id INT NOT NULL, usr_id INT NOT NULL, begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status INT,PRIMARY KEY(order_id));
create table tb1( order_id INT NOT NULL, product_id INT NOT NULL, usr_id INT NOT NULL, begin_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status INT,PRIMARY KEY(order_id));


