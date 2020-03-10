#sudo su -

#export MYSQL55=`docker logs db55 2>&1 | grep GENERATED | sed  's/\[Entrypoint\] GENERATED ROOT PASSWORD\: //'`
#MYSQL55=${MYSQL55//\$/\\\$}
#MYSQL55=${MYSQL55//\&/\\\&}
#MYSQL55=${MYSQL55//\(/\\\(}
#MYSQL55=${MYSQL55//\)/\\\)}
#docker exec db55 bash -c "echo \"SET PASSWORD FOR 'root'@'localhost' = PASSWORD('1qaz@WSX3edc'); CREATE USER 'testuser'@'%' IDENTIFIED BY 'test';GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; GRANT SUPER ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"

sleep 5
docker exec db55 bash -c "echo \"GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"
docker exec db55 bash -c "echo -e \"[client]\nuser=\"root\"\npassword=\"root\"\">/tmp/my.cnf"
docker exec db55 bash -c "mysql --defaults-extra-file=/tmp/my.cnf  </tmp/tmp.sql"
docker exec db55 rm /tmp/tmp.sql
docker exec db55 rm /tmp/my.cnf

#export MYSQL56=`docker logs db56 2>&1 | grep GENERATED | sed  's/\[Entrypoint\] GENERATED ROOT PASSWORD\: //'`
#MYSQL56=${MYSQL56//\$/\\\$}
#MYSQL56=${MYSQL56//\&/\\\&}
#MYSQL56=${MYSQL56//\(/\\\(}
#MYSQL56=${MYSQL56//\)/\\\)}
#docker exec db56 bash -c "echo \"SET PASSWORD FOR 'root'@'localhost' = PASSWORD('1qaz@WSX3edc'); CREATE USER 'testuser'@'%' IDENTIFIED BY 'test';GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; GRANT SUPER ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"
#docker exec db56 bash -c "echo -e \"[client]\nuser=\"root\"\npassword=\"${MYSQL56}\"\">/tmp/my.cnf"
#docker exec db56 bash -c "mysql --defaults-extra-file=/tmp/my.cnf  --connect-expired-password </tmp/tmp.sql"

sleep 5
docker exec db56 bash -c "echo \"GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"
docker exec db56 bash -c "echo -e \"[client]\nuser=\"root\"\npassword=\"root\"\">/tmp/my.cnf"
docker exec db56 bash -c "mysql --defaults-extra-file=/tmp/my.cnf </tmp/tmp.sql"
docker exec db56 rm /tmp/tmp.sql
docker exec db56 rm /tmp/my.cnf

#export MYSQL57=`docker logs db57 2>&1 | grep GENERATED | sed  's/\[Entrypoint\] GENERATED ROOT PASSWORD\: //'`
#MYSQL57=${MYSQL57//\$/\\\$}
#MYSQL57=${MYSQL57//\&/\\\&}
#MYSQL57=${MYSQL57//\(/\\\(}
#MYSQL57=${MYSQL57//\)/\\\)}
#docker exec db57 bash -c "echo \"SET PASSWORD FOR 'root'@'localhost' = '1qaz@WSX3edc'; CREATE USER 'testuser'@'%' IDENTIFIED BY 'test';GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; GRANT SUPER ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"
#docker exec db57 bash -c "echo -e \"[client]\nuser=\"root\"\npassword=\"${MYSQL57}\"\">/tmp/my.cnf"
#docker exec db57 bash -c "mysql --defaults-extra-file=/tmp/my.cnf  --connect-expired-password </tmp/tmp.sql"

sleep 5
docker exec db57 bash -c "echo \"GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"
docker exec db57 bash -c "echo -e \"[client]\nuser=\"root\"\npassword=\"root\"\">/tmp/my.cnf"
docker exec db57 bash -c "mysql --defaults-extra-file=/tmp/my.cnf </tmp/tmp.sql"
docker exec db57 rm /tmp/tmp.sql
docker exec db57 rm /tmp/my.cnf

#export MYSQL80=`docker logs db80 2>&1 | grep GENERATED | sed  's/\[Entrypoint\] GENERATED ROOT PASSWORD\: //'`
#MYSQL80=${MYSQL80//\$/\\\$}
#MYSQL80=${MYSQL80//\&/\\\&}
#MYSQL80=${MYSQL80//\(/\\\(}
#MYSQL80=${MYSQL80//\)/\\\)}
#docker exec db80 bash -c "echo \"SET PASSWORD FOR 'root'@'localhost' = '1qaz@WSX3edc'; CREATE USER 'testuser'@'%' IDENTIFIED BY 'test';GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; GRANT SUPER ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"
#docker exec db80 bash -c "echo -e \"[client]\nuser=\"root\"\npassword=\"${MYSQL80}\"\">/tmp/my.cnf"
#docker exec db80 bash -c "mysql --defaults-extra-file=/tmp/my.cnf  --connect-expired-password </tmp/tmp.sql"

sleep 5
docker exec db80 bash -c "echo \"GRANT ALL PRIVILEGES ON *.* TO 'testuser'@'%'; FLUSH PRIVILEGES;\" > /tmp/tmp.sql"
docker exec db80 bash -c "echo -e \"[client]\nuser=\"root\"\npassword=\"root\"\">/tmp/my.cnf"
docker exec db80 bash -c "mysql --defaults-extra-file=/tmp/my.cnf </tmp/tmp.sql"
docker exec db80 rm /tmp/tmp.sql
docker exec db80 rm /tmp/my.cnf

