#!/bin/bash

mkdir logs
python3 logdb_server.py 95.163.100.29 9011 --loglevel=WARNING --test-queue 2>&1 | tee -a logs/android_log_loader.log &

exit 0;
