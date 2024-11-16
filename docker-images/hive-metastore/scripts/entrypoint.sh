#!/bin/bash
until nc -z -v -w30 yt-mariadb 3306; do
  echo "Waiting for MariaDB..."
  sleep 2
done

/opt/hive/bin/schematool -dbType mysql -initSchema
/opt/hive/bin/hive --service metastore