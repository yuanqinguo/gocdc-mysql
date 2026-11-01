#!/bin/bash
M="docker exec vf-src mysql -uroot -p123456 -h127.0.0.1 -N -e"
$M "SHOW BINLOG EVENTS IN 'binlog.000004'" 2>/dev/null | awk -F'\t' '$3=="Query"' | grep -vE 'CREATE TABLE (shop\.|biz2\.|types\.|rmqtest\.|cluster_e2e\.|IF NOT EXISTS)|CREATE USER|CREATE DATABASE|DROP DATABASE|seed-|user[0-9]|prod-|预插' | head -30
