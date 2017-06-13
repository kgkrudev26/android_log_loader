#!/bin/bash

OLDER_THAN=$(( 60*60*24*7 ))

echo "SET SQL_SAFE_UPDATES = 0; DELETE FROM android.logs_android WHERE packet_time < (unix_timestamp(now()) - $OLDER_THAN);" | mysql --host=178.21.13.216 --port=3306 --user=log_android --password='tk20tk20'

exit 0;
