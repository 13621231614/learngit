#!/bin/bash
 
#Get InnoDB Row Lock Details and InnoDB Transcation Lock Memory
#mysql> SELECT SUM(trx_rows_locked) AS rows_locked, SUM(trx_rows_modified) AS rows_modified, SUM(trx_lock_memory_bytes) AS lock_memory FROM information_schema.INNODB_TRX;
#+-------------+---------------+-------------+
#| rows_locked | rows_modified | lock_memory |
#+-------------+---------------+-------------+
#|        NULL |          NULL |        NULL |
#+-------------+---------------+-------------+
#1 row in set (0.00 sec)
 
#+-------------+---------------+-------------+
#| rows_locked | rows_modified | lock_memory |
#+-------------+---------------+-------------+
#|           0 |             0 |         376 |
#+-------------+---------------+-------------+
 
#Get InnoDB Compression Time
#mysql> SELECT SUM(compress_time) AS compress_time, SUM(uncompress_time) AS uncompress_time FROM information_schema.INNODB_CMP;
#+---------------+-----------------+
#| compress_time | uncompress_time |
#+---------------+-----------------+
#|             0 |               0 |
#+---------------+-----------------+
#1 row in set (0.00 sec)
 
 
#Get InnoDB Transaction states
 
#TRX_STATE  Transaction execution state. One of RUNNING, LOCK WAIT, ROLLING BACK or COMMITTING.
 
#mysql> SELECT LOWER(REPLACE(trx_state, " ", "_")) AS state, count(*) AS cnt from information_schema.INNODB_TRX GROUP BY state;
#+---------+-----+
#| state   | cnt |
#+---------+-----+
#| running |   1 |
#+---------+-----+
#1 row in set (0.00 sec)
 
 
 
 
 
innodb_metric=$1
 
case $innodb_metric in
   Innodb_rows_locked)		#事务锁住的行数
                      value=$(echo "SELECT SUM(trx_rows_locked) AS rows_locked, SUM(trx_rows_modified) AS rows_modified, SUM(trx_lock_memory_bytes) AS lock_memory FROM information_schema.INNODB_TRX;"|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N| awk '{print $1}')
                      if [ "$value" == "NULL" ];then
                         echo 0
                      else
                         echo $value
                      fi
                    ;;
   Innodb_rows_modified)	#事务更改的行数
                      value=$(echo "SELECT SUM(trx_rows_locked) AS rows_locked, SUM(trx_rows_modified) AS rows_modified, SUM(trx_lock_memory_bytes) AS lock_memory FROM information_schema.INNODB_TRX;"|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N| awk '{print $2}')
                      if [ "$value" == "NULL" ];then
                         echo 0
                      else
                         echo $value
                      fi
                    ;;
   Innodb_trx_lock_memory)	#事务锁住的内存大小（B）
                      value=$(echo "SELECT SUM(trx_rows_locked) AS rows_locked, SUM(trx_rows_modified) AS rows_modified, SUM(trx_lock_memory_bytes) AS lock_memory FROM information_schema.INNODB_TRX;"|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N| awk '{print $3}')
                      if [ "$value" == "NULL" ];then
                         echo 0
                      else
                         echo $value
                      fi
                    ;;
      Innodb_compress_time)	#事务压缩时间
                      value=$(echo "SELECT SUM(compress_time) AS compress_time, SUM(uncompress_time) AS uncompress_time FROM information_schema.INNODB_CMP;"|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|awk '{print $1}')
                      echo $value
                      ;;
        
     Innodb_uncompress_time)	#事务解压时间
                      value=$(echo "SELECT SUM(compress_time) AS compress_time, SUM(uncompress_time) AS uncompress_time FROM information_schema.INNODB_CMP;"|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|awk '{print $2}')
                      echo $value
                      ;;   
         Innodb_trx_running)	#事务执行状态数量
                         value=$(echo 'SELECT LOWER(REPLACE(trx_state, " ", "_")) AS state, count(*) AS cnt from information_schema.INNODB_TRX GROUP BY state;'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep running|awk '{print $2}')
                         if [ "$value" == "" ];then
                            echo 0
                         else
                            echo $value
                         fi
                        ;;
       Innodb_trx_lock_wait)	#事务锁等待数量
                         value=$(echo 'SELECT LOWER(REPLACE(trx_state, " ", "_")) AS state, count(*) AS cnt from information_schema.INNODB_TRX GROUP BY state;'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep lock_wait|awk '{print $2}')
                         if [ "$value" == "" ];then
                            echo 0
                         else
                            echo $value
                         fi
                        ;;
    Innodb_trx_rolling_back)	#事务回滚数量
                         value=$(echo 'SELECT LOWER(REPLACE(trx_state, " ", "_")) AS state, count(*) AS cnt from information_schema.INNODB_TRX GROUP BY state;'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep rolling_back|awk '{print $2}')
                         if [ "$value" == "" ];then
                            echo 0
                         else
                            echo $value
                         fi
                        ;;
    Innodb_trx_committing)	#事务提交数量
                         value=$(echo 'SELECT LOWER(REPLACE(trx_state, " ", "_")) AS state, count(*) AS cnt from information_schema.INNODB_TRX GROUP BY state;'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep committing|awk '{print $2}')
                         if [ "$value" == "" ];then
                            echo 0
                         else
                            echo $value
                         fi
                        ;;
 Innodb_trx_history_list_length)	#回滚空间中的未清除事务数。随着事务的提交，它的值会增加；随着清除线程的运行，它的值会减小
                         echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "History list length"|awk '{print $4}'
                        ;;
    Innodb_last_checkpoint_at)		#最后检查日志点
                         echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "Last checkpoint at"|awk '{print $4}'
                        ;;
 
   Innodb_log_sequence_number)		#日志序列号码 相当于Innodb自从表空间开始创建直到现在产生日志文件的总字节数
                         echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "Log sequence number"|awk '{print $4}'
                        ;;
    Innodb_log_flushed_up_to)		#日志刷新点
                         echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "Log flushed up to"|awk '{print $5}'
                        ;;
   Innodb_open_read_views_inside_innodb)	#打开读视图数量
                         echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "read views open inside InnoDB"|awk '{print $1}'
                        ;;
        Innodb_queries_inside_innodb)		#Innodb内部查询线程数量
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "queries inside InnoDB"|awk '{print $1}'
                        ;;
        Innodb_queries_in_queue)		#Innodb内部查询线程队列
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "queries in queue"|awk '{print $5}'
                        ;;
        Innodb_hash_seaches)		#每秒搜索hash索引数量
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "hash searches"|awk '{print $1}'
                        ;;
       Innodb_non_hash_searches)	#每秒搜索non-hash索引
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "non-hash searches/s"|awk '{print $4}'
                        ;;
       Innodb_node_heap_buffers)	#hash索引使用堆节点缓冲区数量
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "node heap"|awk '{print $8}'
                       ;;
       Innodb_mutex_os_waits)		#互斥锁系统等待数量
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "Mutex spin waits"|awk '{print $9}'
                       ;;
       Innodb_mutex_spin_rounds)	#互斥锁自旋轮转数量
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "Mutex spin waits"|awk '{print $6}'|tr -d ','
                       ;;
       Innodb_mutex_spin_waits)		#互斥锁自旋等待数量
                        echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "Mutex spin waits"|awk '{print $4}'|tr -d ','
                       ;; 
       Innodb_file_read)		#数据传输读取IO使用
			echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "avg bytes/read"|awk '{print $1}'
                       ;; 
       Innodb_file_write)		#数据传输写入IO使用
			echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "avg bytes/read"|awk '{print $6}'
                       ;; 
       Innodb_file_fsync)		#数据传输写入硬盘IO使用
			echo 'show engine innodb status\G'|mysql --defaults-file=/usr/local/zabbix/etc/.my.cnf -N|grep "avg bytes/read"|awk '{print $8}'
                       ;; 
 
                   *)
                    echo "wrong parameter"
                    ;;
 
esac
