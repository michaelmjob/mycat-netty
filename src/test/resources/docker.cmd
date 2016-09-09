docker-machine  create -d virtualbox  mysql1
docker-machine env mysql1
# export DOCKER_TLS_VERIFY="1"
# should use the output from last command
# export DOCKER_HOST="tcp://192.168.99.101:2376"
# export DOCKER_CERT_PATH="/Users/snow_young/.docker/machine/machines/mysql1"
# export DOCKER_MACHINE_NAME="mysql1"
# eval $(docker-machine env mysql1)


docker build -t  mysql1:latest  .
docker run --name mysql0  -e MYSQL_ROOT_PASSWORD=xujianhai  -p  3320:3306 -d tutum/mysql:latest
docker exec -it  mysql0  bash

mysql -uroot
SET PASSWORD = PASSWORD("xujianhai");
CREATE USER 'xujianhai'@'192.168.99.1' IDENTIFIED BY 'xujianhai';

# run *.sql, now db0.sql

# 建立了两个
mysql -uroot  -p -h 192.168.99.101 -P 3320
mysql -uroot  -p -h 192.168.99.100 -P 3320

