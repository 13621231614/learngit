#!/bin/bash
nohup /usr/bin/python3 /app/zabbix/scripts/pyora.py --username $1 --password $2 --address $3 --hostname $5 --database $4  table_space &
echo "表空间展示页面更新完成"
