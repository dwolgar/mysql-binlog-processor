cat > /tmp/db55.cnf <<EOF
[mysqld]
skip-host-cache
skip-name-resolve
datadir=/var/lib/mysql
socket=/var/lib/mysql/mysql.sock
secure-file-priv=/var/lib/mysql-files
user=mysql

# Disabling symbolic-links is recommended to prevent assorted security risks
symbolic-links=0

log-error=/var/log/mysqld.log
pid-file=/var/run/mysqld/mysqld.pid

server-id=1
log-bin = binlog
log-bin-index=binlog.index
max_binlog_size=5M
binlog_format=row

EOF