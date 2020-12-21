#!/usr/bin/env python
# coding: utf-8
# vim: tabstop=2 noexpandtab

import argparse
import cx_Oracle
import inspect
import json
import re
import time
from  os import popen

version = 0.2

zabbix_server_ip = '20.34.6.101'
ZABBIX_SEND_ADDR = '/app/zabbix/bin/zabbix_sender'
time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
t = time.localtime(time.time() - 600)
time_old = time.strftime("%Y-%m-%d %H:%M:%S", t)

class Checks(object):

    def daily_rman_info(self):
        """"""
        sql = "select to_char(r.start_time,'YYYY-MM-DD HH24:MI:SS') start_time,\
	    r.time_taken_display elapsed_time,\
	    r.status,\
	    r.input_type input_type,\
	    r.output_device_type output_device_type,\
	    r.output_bytes_display out_size \
	    from (select command_id,start_time,time_taken_display,status,input_type,output_device_type,input_bytes_display,output_bytes_display,output_bytes_per_sec_display\
		from v$rman_backup_job_details order by start_time desc) r\
		where sysdate - start_time <= 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print (res)

    def daily_rman_backup_times(self):
        """"""
        sql = "select count(*) from (select command_id,start_time,time_taken_display,status,input_type,output_device_type,input_bytes_display,output_bytes_display,output_bytes_per_sec_display from v$rman_backup_job_details order by start_time desc) r where sysdate - start_time <= 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print (res[0][0])

    def latest_rman_backup_info(self):
        """"""
        sql = "select to_char(r.start_time,'YYYY-MM-DD HH24:MI:SS') start_time,\
            r.time_taken_display elapsed_time,\
            r.status,\
            r.input_type input_type,\
            r.output_device_type output_device_type,\
            r.output_bytes_display out_size \
            from (select command_id,start_time,time_taken_display,status,input_type,output_device_type,input_bytes_display,output_bytes_display,output_bytes_per_sec_display\
                from v$rman_backup_job_details order by start_time desc) r\
                where sysdate - start_time <= 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        res = "备份开始时间: " + res[0][0] + "\t" + "耗时: " + res[0][1] + "\t" + "结果: " + res[0][2] + "\t" + "类型: " + res[0][3] + "\t" + "介质: " + res[0][4] 
        print(res)
    
    def temp_tablespace(self):
        """"""
        sql = "select c.tablespace_name, \
        to_char(c.bytes / 1024 / 1024 / 1024, '99,999.99') total_gb, \
        to_char((c.bytes - d.bytes_used) / 1024 / 1024 / 1024, '99,999.99') free_gb, \
        to_char(d.bytes_used / 1024 / 1024 / 1024, '99,999.99') use_gb, \
        to_char(d.bytes_used * 100 / c.bytes, '99.99') || '%' use from (select tablespace_name, sum(bytes) bytes from dba_temp_files group by tablespace_name) c, \
        (select tablespace_name, sum(bytes_cached) bytes_used from v$temp_extent_pool group by tablespace_name) d \
        where c.tablespace_name = d.tablespace_name"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        res = "表空间" + "  " + "总空间" + "  " + "剩余空间" + "  " + "使用空间" + "    " + "使用%比" + "\n" + res[0][0] + "" +  res[0][1] +  res[0][2] + "" + res[0][3] + "   " + res[0][4]
        print (res)
    
    def invalid_objects(self):
        """"""
        sql = "select owner,object_type,status,count(object_name) as \"count\" from dba_objects where status='INVALID' group by owner,object_type,status order by count(object_name) desc"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print(res)

    def invalid_indexs(self):
        """"""
        sql = "select owner,index_name,index_type,table_name,status from dba_indexes where status='UNUSABLE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if len(res) > 0:
            print(res)
        print("无效索引数:0")

    def check_active(self):
        """Check Intance is active and open"""
        sql = "select to_char(case when inst_cnt > 0 then 1 else 0 end, \
              'FM99999999999999990') retvalue from (select count(*) inst_cnt \
              from v$instance where status = 'OPEN' and logins = 'ALLOWED' \
              and database_status = 'ACTIVE')"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        #print (res)
        for i in res:
            print(i[0])

    def rcachehit(self):
        """Read Cache hit ratio"""
        sql = "SELECT to_char((1 - (phy.value - lob.value - dir.value) / \
              ses.value) * 100, 'FM99999990.9999') retvalue \
              FROM   v$sysstat ses, v$sysstat lob, \
              v$sysstat dir, v$sysstat phy \
              WHERE  ses.name = 'session logical reads' \
              AND    dir.name = 'physical reads direct' \
              AND    lob.name = 'physical reads direct (lob)' \
              AND    phy.name = 'physical reads'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def dsksortratio(self):
        """Disk sorts ratio"""
        sql = "SELECT to_char(d.value/(d.value + m.value)*100, \
              'FM99999990.9999') retvalue \
              FROM  v$sysstat m, v$sysstat d \
              WHERE m.name = 'sorts (memory)' \
              AND d.name = 'sorts (disk)'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def activeusercount(self):
        """Count of active users"""
        sql = "select to_char(count(*)-1, 'FM99999999999999990') retvalue \
              from v$session where username is not null \
              and status='ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def dbsize(self):
        """Size of user data (without temp)"""
        sql = "SELECT to_char(sum(  NVL(a.bytes - NVL(f.bytes, 0), 0)), \
              'FM99999999999999990') retvalue \
              FROM sys.dba_tablespaces d, \
              (select tablespace_name, sum(bytes) bytes from dba_data_files \
              group by tablespace_name) a, \
              (select tablespace_name, sum(bytes) bytes from \
              dba_free_space group by tablespace_name) f \
              WHERE d.tablespace_name = a.tablespace_name(+) AND \
              d.tablespace_name = f.tablespace_name(+) \
              AND NOT (d.extent_management like 'LOCAL' AND d.contents \
              like 'TEMPORARY')"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def dbfilesize(self):
        """Size of all datafiles"""
        sql = "select to_char(sum(bytes), 'FM99999999999999990') retvalue \
              from dba_data_files"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def version(self):
        """Oracle version (Banner)"""
        sql = "select banner from v$version where rownum=1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def uptime(self):
        """Instance Uptime (seconds)"""
        sql = "select to_char((sysdate-startup_time)*86400, \
              'FM99999999999999990') retvalue from v$instance"
        self.cur.execute(sql)
        res = self.cur.fetchmany(numRows=3)
        for i in res:
            print(i[0])

    def commits(self):
        """User Commits"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'user commits'"
        self.cur.execute(sql)
        res = self.cur.fetchmany(numRows=3)
        for i in res:
            print(i[0])

    def rollbacks(self):
        """User Rollbacks"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from " \
              "v$sysstat where name = 'user rollbacks'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def deadlocks(self):
        """Deadlocks"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'enqueue deadlocks'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def deadlocksuserlock(self):
        """哪个用户锁定了哪个对象"""
        #sql = "select username,lockwait,status,machine,program from v$session where sid in (select session_id from v$locked_object)"
        #sql = "SELECT /*+ rule */ s.username,decode(l.type,'TM','TABLE LOCK','TX','ROW LOCK',NULL) LOCK_LEVEL,o.owner,o.object_name,o.object_type, s.sid,s.serial#,s.terminal,s.machine,s.program,s.osuser FROM v$session s,v$lock l,dba_objects o WHERE l.sid = s.sid AND l.id1 = o.object_id(+) AND s.username is NOT Null"
        sql = "SELECT s.sid, s.serial#, s.username, s.schemaname, s.osuser, s.process, s.machine,s.terminal, s.logon_time, l.type FROM v$session s, v$lock l WHERE s.sid = l.sid AND s.username IS NOT NULL ORDER BY sid"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def deadlocksinfo(self):
        """锁情况查看"""
        sql = "select s.username,decode(request,0,'Holder:',' Waiter:') || ':' || s.sid||','|| s.serial# sess,id1, id2, lmode, request, l.type, ctime, s.sql_id, s.event,s.last_call_et from v$lock l, v$session s where (id1, id2, l.type) in (select id1, id2, type from v$lock where request>0) and l.sid=s.sid order by id1, ctime desc, request"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def deadlocksstatus(self):
        """Locked status check 被锁对象查看"""
        sql = "select a.LOCKED_MODE,b.owner,b.object_name,a.object_id,a.session_id,c.serial#,c.username,c.sql_id,c.PROCESS ,c.PROGRAM \
              from v$locked_object  a, dba_objects b,v$session c  where a.object_id=b.object_id and c.sid=a.session_id"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def invalidobject(self):
        """Invalid object 无效对象"""
        sql = "select object_name,object_type,owner,created,last_ddl_time,timestamp from dba_objects where status='INVALID' order by 1,2"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def invalidobjectindex(self):
        """Invalid object 无效索引"""
        sql = "select index_name,table_name,tablespace_name,status From dba_indexes Where status<>'VALID'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print(sql)
        for i in res:
            print(i[:])

    def invalidobjecttrigger(self):
        """Invalid object 无效触发器"""
        sql = "SELECT owner, trigger_name, table_name, status FROM dba_triggers WHERE status = 'DISABLED'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print(sql)
        for i in res:
            print(i[:])


    def invalidobjectrestraints(self):
        """Invalid object 无效约束"""
        sql = "SELECT owner, constraint_name, table_name, constraint_type, status FROM dba_constraints WHERE status = 'DISABLED'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print(sql)
        for i in res:
            print(i[:])

    def occupancymen(self):
        """查看消耗内存多的sql"""
        sql = "select b.username,a. buffer_gets, a.executions,  a.disk_reads / decode(a.executions, 0, 1, a.executions), a.sql_text SQL  from v$sqlarea a, dba_users b where a.parsing_user_id = b.user_id   and a.disk_reads > 10000 order by disk_reads desc"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        data = []
        for i in res:
            print(i[:])

    def occupancymore(self):
        """使用频率最高的5个查询 top5"""
        sql = "select sql_text,executions from (select sql_text,executions, rank() over (order by executions desc) exec_rank from v$sql) where exec_rank <=5"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[:])

    def occupancydisk(self):
        """消耗磁盘读取最多的sql top5"""
        sql = "select disk_reads,sql_text from (select sql_text,disk_reads, dense_rank() over (order by disk_reads desc) disk_reads_rank from v$sql) where disk_reads_rank <=5"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[:])

    def occupancysort(self):
        """查看排序多的SQL"""
        sql = "select sql_text, sorts from(select sql_text, sorts from v$sqlarea order by sorts desc) where rownum<21"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[:])

    def occupancyanalysis(self):
        """分析的次数太多，执行的次数太少，要用绑变量的方法来写sql"""
        sql = "select substr(sql_text, 1, 80) , count(*), sum(executions) \
from v$sqlarea \
where executions < 5 \
group by substr(sql_text, 1, 80) \
having count(*) > 30 \
order by 2"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[:])

    def occupancycache(self):
        """大量缓冲读取（逻辑读）操作的查询top5"""
        sql = "select buffer_gets,sql_text from (select sql_text,buffer_gets, dense_rank() over (order by buffer_gets desc) buffer_gets_rank from v$sql) where buffer_gets_rank<=5"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[:])

    def deadlocksstatussql(self):
        """查看到被死锁的语句"""
        sql = "select sql_text from v$sql where hash_value in (select sql_hash_value from v$session where sid in (select session_id from v$locked_object))"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])

    def occupancycpu(self):
        """查询当前系统耗费CPU资源最多sql top5"""
        sql = "select buffer_gets,sql_text from (select sql_text,buffer_gets, dense_rank() over (order by buffer_gets desc) buffer_gets_rank from v$sql) where buffer_gets_rank<=5"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[1])

    def deadloclprocess(self):
        """查找死锁的进程"""
        sql = "SELECT s.username,l.OBJECT_ID,l.SESSION_ID,s.SERIAL#,l.ORACLE_USERNAME,l.OS_USER_NAME,l.PROCESS \
              FROM V$LOCKED_OBJECT l,V$SESSION S WHERE l.SESSION_ID=S.SID"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])

    def redowrites(self):
        """Redo Writes"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'redo writes'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def tblscans(self):
        """Table scans (long tables)"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'table scans (long tables)'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def tblrowsscans(self):
        """Table scan rows gotten"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'table scan rows gotten'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def indexffs(self):
        """Index fast full scans (full)"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'index fast full scans (full)'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def hparsratio(self):
        """Hard parse ratio"""
        try:
            sql = "SELECT to_char(h.value/t.value*100,'FM99999990.9999') retvalue FROM  v$sysstat h, v$sysstat t WHERE h.name = 'parse count (hard)' AND t.name = 'parse count (total)'"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            for i in res:
                print(i[0])
        except Exception as ex:
            print(ex)

    def netsent(self):
        """Bytes sent via SQL*Net to client"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'bytes sent via SQL*Net to client'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def netresv(self):
        """Bytes received via SQL*Net from client"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'bytes received via SQL*Net from client'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def netroundtrips(self):
        """SQL*Net roundtrips to/from client"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'SQL*Net roundtrips to/from client'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def logonscurrent(self):
        """Logons current"""
        sql = "select to_char(value, 'FM99999999999999990') retvalue from \
              v$sysstat where name = 'logons current'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def lastarclog(self):
        """Last archived log sequence"""
        sql = "select to_char(max(SEQUENCE#), 'FM99999999999999990') \
              retvalue from v$log where archived = 'YES'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def lastapplarclog(self):
        """Last applied archive log (at standby).Next items requires
        [timed_statistics = true]"""
        sql = "select to_char(max(lh.SEQUENCE#), 'FM99999999999999990') \
              retvalue from v$loghist lh, v$archived_log al \
              where lh.SEQUENCE# = al.SEQUENCE# and applied='YES'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def freebufwaits(self):
        """Free buffer waits"""
        try:
            sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
                  from v$system_event se, v$event_name en \
                  where se.event(+) = en.name and en.name = 'free buffer waits'"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            if len(res) == 0:
                print("0")
            else:
                for i in res:
                    print(i[0])
        except Exception as err:
            print(err)

    def bufbusywaits(self):
        """Buffer busy waits"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) = \
              en.name and en.name = 'buffer busy waits'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def logswcompletion(self):
        """log file switch completion"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'log file switch completion'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def logfilesync(self):
        """Log file sync"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en \
              where se.event(+) = en.name and en.name = 'log file sync'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def logprllwrite(self):
        """Log file parallel write"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'log file parallel write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def enqueue(self):
        """Enqueue waits"""
        try:
            sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
                  from v$system_event se, v$event_name en \
                  where se.event(+) = en.name and en.name = 'enqueue'"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            if len(res) == 0:
                print("0")
            else:
                for i in res:
                    print(i[0])
        except Exception as err:
            print(str(err))

    def dbseqread(self):
        """DB file sequential read waits"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'db file sequential read'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def dbscattread(self):
        """DB file scattered read"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'db file scattered read'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def dbsnglwrite(self):
        """DB file single write"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'db file single write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def dbprllwrite(self):
        """DB file parallel write"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'db file parallel write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def directread(self):
        """Direct path read"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'direct path read'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def directwrite(self):
        """Direct path write"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'direct path write'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def latchfree(self):
        """latch free"""
        sql = "select to_char(time_waited, 'FM99999999999999990') retvalue \
              from v$system_event se, v$event_name en where se.event(+) \
              = en.name and en.name = 'latch free'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def tablespace(self, name):
        """Get tablespace usage"""
        sql = "SELECT df.tablespace_name TABLESPACE,  \
        ROUND ( (df.bytes - SUM (fs.bytes)) * 100 / df.bytes, 2) \
        USED FROM  (SELECT TABLESPACE_NAME,BYTES FROM  \
        sys.sm$ts_free fs UNION ALL SELECT TABLESPACE_NAME,FREE_SPACE  FROM DBA_TEMP_FREE_SPACE ) FS, \
        (SELECT tablespace_name, SUM (bytes) bytes FROM sys.sm$ts_avail GROUP BY  tablespace_name UNION ALL SELECT TABLESPACE_NAME, SUM(bytes) \
        FROM SYS.DBA_TEMP_FILES GROUP BY tablespace_name ) df  WHERE fs.tablespace_name(+) = df.tablespace_name \
        AND df.tablespace_name = '{0}' GROUP BY df.tablespace_name,df.bytes ORDER BY 1".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[1])

    def autousage(self, name):
        """Get tablespace auto usage"""
        sql = "SELECT a.tablespace_name TABLESPACE,round(((a.total - b.free) / c.AUTOSIZE), 4) * 100 as AUTOUSAGEPER  from (select tablespace_name, sum(nvl(bytes, 2)) / 1024 / 1024 total from dba_data_files group by tablespace_name) a,(select tablespace_name, sum(nvl(bytes, 2)) / 1024 / 1024 free from dba_free_space group by tablespace_name) b,(select x.TABLESPACE_NAME, sum(x.AUTOSIZE) AUTOSIZE from (select TABLESPACE_NAME,CASE WHEN MAXBYTES / 1024 / 1024 = 0 THEN BYTES / 1024 / 1024 ELSE MAXBYTES / 1024 / 1024 END AUTOSIZE from DBA_DATA_FILES) x group by x.tablespace_name) c where a.tablespace_name = b.tablespace_name and a.tablespace_name = c.tablespace_name and b.TABLESPACE_NAME = c.TABLESPACE_NAME and a.tablespace_name = '{0}' ORDER BY 1".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[1])

    def show_tablespaces(self):
        """List tablespace names in a JSON like format for Zabbix use"""
        sql = "SELECT tablespace_name FROM dba_tablespaces ORDER BY 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#TABLESPACE}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print(json.dumps({'data': lst}))

    def show_tablespaces_temp(self):
        """List temporary tablespace names in a JSON like
        format for Zabbix use"""
        sql = "SELECT tablespace FROM V$TEMPSEG_USAGE group by tablespace \
              ORDER BY 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#TABLESPACE_TEMP}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print(json.dumps({'data': lst}))

    def query_temp(self):
        """query tablespaces temp"""
        sql = "select sum(bytes) \"temp size(B)\" from dba_temp_files \
							where tablespace_name='TEMP'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])


    def check_archive(self, archive):
        """List archive used"""
        sql = "select trunc((total_mb-free_mb)*100/(total_mb)) PCT from \
              v$asm_diskgroup_stat where name='{0}' \
              ORDER BY 1".format(archive)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def show_asm_volumes(self):
        """List als ASM volumes in a JSON like format for Zabbix use"""
        sql = "select NAME from v$asm_diskgroup_stat ORDER BY 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#ASMVOLUME}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print(json.dumps({'data': lst}))

    def asm_volume_use(self, name):
        """Get ASM volume usage"""
        sql = "select round(((TOTAL_MB-FREE_MB)/TOTAL_MB*100),2) from \
              v$asm_diskgroup_stat where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def query_lock(self):
        """Query lock"""
        sql = "SELECT count(*) FROM gv$lock l WHERE  block=1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def query_lockobjok(self):
        """首先查看被Lock的object id"""
        sql = "Select gob.*,gp.spid From gv$locked_object gob ,gv$session gs,gv$process gp Where gob.session_id=gs.sid And gs.paddr=gp.addr And gob.inst_id=gs.inst_id And gob.inst_id=gp.inst_id"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def query_lockuserlock(self):
        """哪个用户在锁表"""
        sql = "select a.username, a.sid, a.serial#, b.id1 from gv$session a, gv$lock b where a.lockwait = b.kaddr"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])

    def query_lockproess(self):
        """查询阻塞的进程，被阻塞的进程"""
        sql = "select sid,blocking_session from gv$session where blocking_session is not null"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def query_lockinfo(self):
        """阻塞详情"""
        sql = "SELECT DISTINCT  s1.username || '@' || s1.machine || ' ( INST=' || s1.inst_id || ' SID=' || s1.sid || ' Serail#=' || s1.serial# || ' ) IS BLOCKING ' || s2.username || '@' || s2.machine || ' ( INST=' || s2.inst_id || ' SID=' || s2.sid || ' Serial#=' || s2.serial# || ' ) '\
  AS blocking_status FROM gv$lock l1, gv$session s1, gv$lock l2, gv$session s2 WHERE   s1.sid = l1.sid AND s2.sid = l2.sid AND s1.inst_id = l1.inst_id AND s2.inst_id = l2.inst_id AND l1.block > 0 AND l2.request > 0 AND l1.id1 = l2.id1 AND l1.id2 = l2.id2"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def query_locksql(self):
        """查询阻塞语句与时间"""
        sql = "SELECT  'sid=' || a.SID || ' Wait Class=' || a.wait_class || ' Time=' || a.seconds_in_wait || ' Query=' || b.sql_text FROM v$session a, v$sqlarea b WHERE a.blocking_session IS NOT NULL AND a.sql_address = b.address ORDER BY a.blocking_session"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def query_lockownerinfo(self):
        """查询阻塞时锁的持有详细信息"""
        sql = "SELECT sn.username, m.sid, m.type,DECODE(m.lmode, 0, 'None', 1, 'Null', 2, 'Row Share', 3, 'Row Excl.', 4, 'Share', 5, 'S/Row Excl.', 6, 'Exclusive',lmode, trim(to_char(lmode,'990'))) lmode,DECODE(m.request,0, 'None',  1, 'Null',  2, 'Row Share',  3, 'Row Excl.',  4, 'Share',  5, 'S/Row Excl.',  6, 'Exclusive',  request, ltrim(to_char(m.request,'990'))) request, m.id1, m.id2 FROM v$session sn, v$lock m WHERE (sn.sid = m.sid AND m.request != 0)  OR (sn.sid = m.sid    AND m.request = 0 AND lmode != 4    AND (id1, id2) IN (SELECT s.id1, s.id2  FROM v$lock s WHERE request != 0    AND s.id1 = m.id1   AND s.id2 = m.id2)   ) ORDER BY id1, id2, m.request"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        if res:
            print('监控执行语句' + ':' + sql)
        else:
            print(str('Null'))
        for i in res:
            print(i[:])


    def query_redologs(self):
        """Redo logs"""
        sql = "select COUNT(*) from v$LOG WHERE STATUS='ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def query_rollbacks(self):
        """Query Rollback"""
        sql = "select nvl(trunc(sum(used_ublk*4096)/1024/1024),0) from \
              gv$transaction t,gv$session s where ses_addr = saddr"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def query_sessions(self):
        """Query Sessions"""
        sql = "select count(*) from gv$session where username is not null \
              and status='ACTIVE'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def tablespace_temp(self, name):
        """Query temporary tablespaces"""
        sql = "SELECT round(sum(a.blocks*8192)*100/bytes,2) percentual FROM \
              V$TEMPSEG_USAGE a, dba_temp_files b where tablespace_name= \
              '{0}' and a.tablespace=b.tablespace_name group by \
              a.tablespace,b.bytes".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def query_sysmetrics(self, name):
        """Query v$sysmetric parameters"""
        sql = "select value from v$sysmetric where METRIC_NAME ='{0}' and \
              rownum <=1 order by INTSIZE_CSEC".format(name.replace('_', ' '))
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def fra_use(self):
        """Query the Fast Recovery Area usage"""
        sql = "select round((SPACE_LIMIT-(SPACE_LIMIT-SPACE_USED))/ \
              SPACE_LIMIT*100,2) FROM V$RECOVERY_FILE_DEST"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def show_users(self):
        """Query the list of users on the instance"""
        sql = "SELECT username FROM dba_users ORDER BY 1"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        key = ['{#DBUSER}']
        lst = []
        for i in res:
            d = dict(zip(key, i))
            lst.append(d)
        print(json.dumps({'data': lst}))

    def user_status(self, dbuser):
        """Determines whether a user is locked or not"""
        sql = "SELECT account_status FROM dba_users WHERE username='{0}'" \
            .format(dbuser)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_pga(self):
        """Query PGA"""
        sql = "select to_char(decode( unit,'bytes', value/1024/1024, value), \
                '999999999.9') value from V$PGASTAT where name in 'total PGA inuse'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_pga_aggregate_target(self):
        """Query PGA aggregate target"""
        sql = "select to_char(decode( unit,'bytes', value/1024/1024, value),'999999999.9') \
                value from V$PGASTAT where name in 'aggregate PGA target parameter'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_sga_buffer_cache(self):
        """Query sga buffer cache"""
        sql = "SELECT to_char(ROUND(SUM(decode(pool,NULL,decode(name,'db_block_buffers',(bytes), \
                'buffer_cache',(bytes),0),0)),2)) sga_bufcache FROM V$SGASTAT"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_sga_fixed(self):
        """Query sga fixed"""
        sql = "SELECT TO_CHAR(ROUND(SUM(decode(pool,NULL,decode(name,'fixed_sga', \
                (bytes),0),0)),2)) sga_fixed FROM V$SGASTAT"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_sga_java_pool(self):
        """Query sga java pool"""
        sql = "SELECT to_char(ROUND(SUM(decode(pool,'java pool',(bytes),0)),2)) \
                sga_jpool FROM V$SGASTAT"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_sga_large_pool(self):
        """Query sga large pool"""
        sql = "SELECT to_char(ROUND(SUM(decode(pool,'large pool',\
                (bytes),0)),2)) sga_lpool FROM V$SGASTAT"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_sga_log_buffer(self):
        """Query sga log buffer"""
        sql = "SELECT TO_CHAR(ROUND(SUM(decode(pool,NULL,decode(name,'log_buffer',\
                (bytes),0),0)),2)) sga_lbuffer FROM V$SGASTAT"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def query_sga_shared_pool(self):
        """Query sga shared pool"""
        sql = "SELECT TO_CHAR(ROUND(SUM(decode(pool,'shared pool',decode(name,'library cache',0,\
                'dictionary cache',0,'free memory',0,'sql area',0,(bytes)),0)),2)) pool_misc FROM V$SGASTAT"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def lio_block_changes(self):
        """Logical I/O block changes"""
        sql = "SELECT to_char(SUM(DECODE(NAME,'db block changes',VALUE,0))) \
                FROM V$SYSSTAT WHERE NAME ='db block changes'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def lio_consistent_read(self):
        """Logical I/O consistent read"""
        sql = "SELECT to_char(sum(decode(name,'consistent gets',value,0))) \
                FROM V$SYSSTAT WHERE NAME ='consistent gets'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def lio_current_read(self):
        """Logical I/O current read"""
        sql = "SELECT to_char(sum(decode(name,'db block gets',value,0))) \
                FROM V$SYSSTAT WHERE NAME ='db block gets'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def phio_datafile_reads(self):
        """Physical I/O datafile reads"""
        sql = "select to_char(sum(decode(name,'physical reads direct',value,0))) \
                FROM V$SYSSTAT where name ='physical reads direct'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def phio_datafile_writes(self):
        """Physical I/O datafile writes"""
        sql = "select to_char(sum(decode(name,'physical writes direct',value,0))) \
                FROM V$SYSSTAT where name ='physical writes direct'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def phio_redo_writes(self):
        """Physical I/O redo writes"""
        sql = "select to_char(sum(decode(name,'redo writes',value,0))) \
                FROM V$SYSSTAT where name ='redo writes'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def connect_audit(self):
        sql = "select username \"username\", \
                to_char(timestamp,'DD-MON-YYYY HH24:MI:SS') \"time_stamp\", \
                action_name \"statement\", \
                os_username \"os_username\", \
                userhost \"userhost\", \
                returncode||decode(returncode,'1004','-Wrong Connection','1005','-NULL Password','1017','-Wrong Password','1045','-Insufficient Priviledge','0','-Login Accepted','--') \"returncode\" \
                from sys.dba_audit_session \
                where (sysdate - timestamp)*24 < 1 and returncode > 0 \
                order by timestamp"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        #print(str('test result'))
        #with open(frequency,'w+') as f:
        #     f.write(str('1') + time.strftime('%Y-%m-%d',time.localtime(time.time())))
        for i in res:
            count = 0
            while count < len(i):
                print(i[count], ' ')
                count = count + 1
            print("\t")

    def process_number(self):
        """Current process number"""
        sql = "select count(*) from V$process"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def process_max_number(self):
        """System process Preset number"""
        sql = "select value from v$system_parameter where name = 'processes'"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def anto_extensible(self, name):
        """Check tablespace auto extensible"""
        sql = "select tablespace_name, autoextensible from dba_data_files where tablespace_name='{0}' group by tablespace_name,autoextensible".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[1])

    def anto_extensible_max(self, name):
        """Check tablespace auto extensible max (包含自动扩展和非自动扩展的值)"""
        sql = "SELECT DD.TABLESPACE_NAME,ROUND(SUM(DECODE(DD.MAXBYTES, 0, DD.BYTES, DD.MAXBYTES)),0) MAX_BYTES_M \
                FROM SYS.DBA_DATA_FILES DD WHERE TABLESPACE_NAME='{0}' \
                GROUP BY DD.TABLESPACE_NAME".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[1])

    def tablespace_user_size(self, name):
        """Check tablespace use size(数值形式)"""
        sql = "SELECT df.tablespace_name TABLESPACE, ROUND ( (df.bytes - SUM (fs.bytes))/1024/1024, 2) USED_M \
                FROM (SELECT TABLESPACE_NAME,BYTES FROM sys.sm$ts_free fs \
                    UNION ALL \
                    SELECT TABLESPACE_NAME,FREE_SPACE  FROM DBA_TEMP_FREE_SPACE ) FS, \
                    (SELECT tablespace_name, SUM (bytes) bytes FROM sys.sm$ts_avail GROUP BY  tablespace_name \
                    UNION ALL \
                    SELECT TABLESPACE_NAME, SUM(bytes) FROM SYS.DBA_TEMP_FILES GROUP BY tablespace_name ) df \
                    WHERE fs.tablespace_name(+) = df.tablespace_name \
                    AND df.tablespace_name = '{0}' GROUP BY df.tablespace_name,df.bytes ORDER BY 1".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[1])

    def tab_cur_total_space(self, name):
        """Check tablespace Current total space(检查目前总空间大小，自动扩展后会增加)"""
        sql = "SELECT b.tablespace_name TABLESPACE, b.bytes/(1024*1024) BYTES FROM \
                (SELECT tablespace_name, SUM (bytes) bytes FROM sys.sm$ts_avail \
                GROUP BY  tablespace_name UNION ALL SELECT TABLESPACE_NAME, SUM(bytes) BYTES \
                FROM SYS.DBA_TEMP_FILES GROUP BY tablespace_name) b WHERE b.tablespace_name='{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[1])
			
    def asm_diskname_total(self, name):
        """ ASM磁盘大小MB"""
        sql = "select TOTAL_MB from v$asm_diskgroup where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])			

    def asm_diskname_used(self, name):
        """ ASM磁盘已使用大小MB"""
        sql = "select round((TOTAL_MB-FREE_MB),2) from v$asm_diskgroup where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def asm_diskname_free(self, name):
        """ ASM磁盘已使用大小MB"""
        sql = "select FREE_MB from v$asm_diskgroup where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def asm_diskname_state(self, name):
        """ ASM磁盘state"""
        sql = "select STATE from v$asm_diskgroup where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def asm_diskname_type(self, name):
        """ ASM磁盘 type"""
        sql = "select TYPE from v$asm_diskgroup where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])
    
    def asm_diskname_offline_disks(self, name):
        """ ASM磁盘state"""
        sql = "select OFFLINE_DISKS from v$asm_diskgroup where name = '{0}'".format(name)
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def sga_hit_ratio(self):
        """ 监控SGA命中率大于90%以上，小于90告警"""
        sql = "select round(100 * ((a.value+b.value)-c.value) / (a.value+b.value)) \"BUFFER HIT RATIO\" \
        from v$sysstat a, v$sysstat b, v$sysstat c \
        where a.statistic# = 38 and \
        b.statistic# = 39 and \
        c.statistic# = 40 "
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def library_used_ratio(self):
        """ 共享SQL区的使用率,大于90%以上，小于90告警"""
        sql = "select round((sum(pins-reloads))/sum(pins),2)*100 \"Library cache\" from v$librarycache"
        self.cur.execute(sql)
        res = self.cur.fetchall()
        for i in res:
            print(i[0])

    def table_space(self):
        arg = self.args
        hostname = arg.hostname
        '''前端页面查看表空间使用'''
        #sql = 'select t.* from (SELECT D.TABLESPACE_NAME "space_name",SPACE "total(M)",BLOCKS "block_num",SPACE - NVL(FREE_SPACE,0) "used(M)",ROUND((1 - NVL(FREE_SPACE, 0) / SPACE) * 100, 2) "used_per(%)",nvl(FREE_SPACE,0) "remain(M)" FROM(SELECT TABLESPACE_NAME,ROUND(SUM(BYTES) / (1024 * 1024), 2) SPACE,SUM(BLOCKS) BLOCKS FROM DBA_DATA_FILES GROUP BY TABLESPACE_NAME) D,(SELECT TABLESPACE_NAME,ROUND(SUM(BYTES) / (1024 * 1024), 2) FREE_SPACE FROM DBA_FREE_SPACE GROUP BY TABLESPACE_NAME) F WHERE D.TABLESPACE_NAME = F.TABLESPACE_NAME(+) UNION ALL SELECT D.TABLESPACE_NAME,SPACE "total(M)",BLOCKS SUM_BLOCKS,nvl(USED_SPACE,0) "used(M)",ROUND(NVL(USED_SPACE, 0) / SPACE * 100, 2) "used_per(%)",nvl((SPACE - USED_SPACE),0) "FREE_SPACE(M)" FROM (SELECT TABLESPACE_NAME,ROUND(SUM(BYTES) / (1024 * 1024), 2) SPACE,SUM(BLOCKS) BLOCKS FROM DBA_TEMP_FILES GROUP BY TABLESPACE_NAME) D,(SELECT TABLESPACE,ROUND(SUM(BLOCKS * 8192) / (1024 * 1024), 2) USED_SPACE FROM V$SORT_USAGE GROUP BY TABLESPACE) F WHERE D.TABLESPACE_NAME = F.TABLESPACE(+)) t order by "used_per(%)" desc'
        #sql = 'select t.* from (SELECT D.TABLESPACE_NAME "space_name",D.MAX_SPACE,SPACE "total(M)",BLOCKS "block_num",SPACE - NVL(FREE_SPACE,0) "used(M)",ROUND((1 - NVL(FREE_SPACE, 0) / SPACE) * 100, 2) "used_per(%)",nvl(FREE_SPACE,0) "remain(M)" FROM(SELECT TABLESPACE_NAME,ROUND(SUM(BYTES) / (1024 * 1024), 2) SPACE,ROUND(SUM(DECODE(MAXBYTES, 0, BYTES, MAXBYTES)),0) MAX_SPACE ,SUM(BLOCKS) BLOCKS FROM DBA_DATA_FILES GROUP BY TABLESPACE_NAME) D,(SELECT TABLESPACE_NAME,ROUND(SUM(BYTES) / (1024 * 1024), 2) FREE_SPACE FROM DBA_FREE_SPACE GROUP BY TABLESPACE_NAME) F WHERE D.TABLESPACE_NAME = F.TABLESPACE_NAME(+) UNION ALL SELECT D.TABLESPACE_NAME,D.MAX_SPACE,SPACE "total(M)",BLOCKS SUM_BLOCKS,nvl(USED_SPACE,0) "used(M)",ROUND(NVL(USED_SPACE, 0) / SPACE * 100, 2) "used_per(%)",nvl((SPACE - USED_SPACE),0) "FREE_SPACE(M)" FROM (SELECT TABLESPACE_NAME,ROUND(SUM(BYTES) / (1024 * 1024), 2) SPACE,ROUND(SUM(DECODE(MAXBYTES, 0, BYTES, MAXBYTES)),0) MAX_SPACE ,SUM(BLOCKS) BLOCKS FROM DBA_TEMP_FILES GROUP BY TABLESPACE_NAME) D,(SELECT TABLESPACE,ROUND(SUM(BLOCKS * 8192) / (1024 * 1024), 2) USED_SPACE FROM V$SORT_USAGE GROUP BY TABLESPACE) F WHERE D.TABLESPACE_NAME = F.TABLESPACE(+)) t order by "used_per(%)" desc'
        sql='select a.tablespace_name,trunc(a.total) allocated_space_mb,trunc(a.total - b.free) Used_mb,trunc(b.free) free_space_mb,round((1 - b.free / a.total), 4) * 100 "USAGE_%",c.AUTOSIZE AUTOSIZE_mb,round(((a.total - b.free) / c.AUTOSIZE), 4) * 100 "AUTOUSAGE_%" from (select tablespace_name, sum(nvl(bytes, 2)) / 1024 / 1024 total from dba_data_files group by tablespace_name) a,(select tablespace_name, sum(nvl(bytes, 2)) / 1024 / 1024 free from dba_free_space group by tablespace_name) b,(select x.TABLESPACE_NAME, sum(x.AUTOSIZE) AUTOSIZE from (select TABLESPACE_NAME,CASE WHEN MAXBYTES / 1024 / 1024 = 0 THEN BYTES / 1024 / 1024 ELSE MAXBYTES / 1024 / 1024 END AUTOSIZE from DBA_DATA_FILES) x group by x.tablespace_name) c where a.tablespace_name = b.tablespace_name and a.tablespace_name = c.tablespace_name and b.TABLESPACE_NAME = c.TABLESPACE_NAME order by 3 desc'
        self.cur.execute(sql)
        res = self.cur.fetchall()
        # print (res)
        if len(res) == 0:
            print ('null')
        else:
            a = json.dumps(res, sort_keys=True,ensure_ascii=False)
            #print (a)
            popen ('%s -z %s -s %s -k %s -o %s > /dev/null ' % (ZABBIX_SEND_ADDR, zabbix_server_ip,"'"+ str(hostname) + "'","'"+'pyora[{$USERNAME},{$PASSWORD},{$ADDRESS},{$DATABASE},table_space]'+"'", "'" + str(a) + "'"))


    def deal_lock(self):
        '''前端页面查看阻塞使用'''
        sql = 'SELECT a.BLOCKER_INSTANCE,a.BLOCKER_SID,a.BLOCKER_SESS_SERIAL#,a.INSTANCE,a.sid,a.SESS_SERIAL#,b.STATUS,b.MACHINE,b.MODULE,b.ACTION,b.SQL_ID,a.in_wait_secs,substr(wait_event_text, 1, 30) as wait_event_text FROM v$wait_chains a ,gv$session b where  a.BLOCKER_SID=b.sid and (instance, a.SID) in (select l.inst_id, l.SID from gv$lock l, gv$session s, gv$process p where l.REQUEST > 3 and l.inst_id = s.inst_id and p.inst_id = s.inst_id and l.sid = s.sid and p.addr = s.paddr)'
        self.cur.execute(sql)
        res = self.cur.fetchall()
        #print (res)
        if len(res) == 0:
            print ('null')
        else:
            print (json.dumps(res, sort_keys=True,ensure_ascii=False)) 	

    def session_top10(self):
        '''前端页面查看会话TOP10使用'''
        sql = 'select * from (select count(a.status) as "active_sql_num",a.SCHEMANAME as "username",a.PROGRAM as "program" from v$session a left join v$sql b on a.SQL_ADDRESS= b.ADDRESS group by a.SCHEMANAME,a.PROGRAM order by "active_sql_num" desc) where rownum <11'
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print (json.dumps(res, sort_keys=True,ensure_ascii=False))

    def slow_sql(self):
        '''前端页面查看慢SQL使用'''
        #sql = 'select * from (select sql_text as "sql_text" ,parsing_schema_name as "parsing_schema_name" ,executions as "executions" , locked_total as "locked_total"  ,round(cpu_time/1000000,2) as "cpu_time",sorts as "sorts" ,buffer_gets as "buffer_gets" ,disk_reads as "disk_reads"  from V$SQL where LAST_LOAD_TIME between  \'%s\' AND \'%s\'  order by cpu_time desc) where rownum < 11' % (time_old, time_now)
        sql = "select * from (select sql_id,username,to_char(sql_exec_start, 'yyyy-mm-dd hh24:mi:ss') sql_exec_start,sql_exec_id,SUBSTR(t.SQL_TEXT,1,300) SQL_TEXT,round(t.ELAPSED_TIME / 1000000,2) as ELAPSED_TIME_secs,sum(buffer_gets) buffer_gets,sum(disk_reads) disk_reads,round(sum(cpu_time / 1000000), 1) cpu_secs from gv$sql_monitor t where username not in ('SYS', 'SYSTEM') and status='EXECUTING' group by sql_id,username,sql_exec_start,sql_exec_id,SQL_TEXT,t.ELAPSED_TIME order by 6 desc) where rownum <= 10"
	#print time_old
        #print time_now
        #print sql
        self.cur.execute(sql)
        res = self.cur.fetchall()
        #print (res)
        if len(res) == 0:
            print ('null')
        else:
            print (json.dumps(res, sort_keys=True,ensure_ascii=False))

    def oracle_DG(self, name):
        '''查看DG状态，大于20告警'''
        if name == 'DG1':
            sql = 'select max(a.sequence#) - max(b.sequence#) from v$log a, v$archived_log b'
            self.cur.execute(sql)
            res = self.cur.fetchall()
            #print (res[0])
            for i in res[0]:
                if i < 20:
                    print ('OK')
                else:
                    print (i)
        elif name == 'DG2':
            sql = ["select max(a.sequence#) - max(b.sequence#) from v$log a, v$archived_log b where a.thread# = '1' and b.thread# = '1'",
                   "select max(a.sequence#) - max(b.sequence#) from v$log a, v$archived_log b where a.thread# = '2' and b.thread# = '2'"
                  ]
            #print (sql)
            dg = []
            for i in sql:
                self.cur.execute(i)
                res = self.cur.fetchall()
                #print (res)
                for i in res[0]:
                    #print i
                    if i > 20:
                        dg.append(i)
            #dg.append(20)
            #print (dg)
            if len(dg) > 1:
                print (dg)
            else:
                print ('OK')
            #print (dg)

    def instance_status(self):
        ''''''
        sql = 'select status from v$instance'
        self.cur.execute(sql)
        res = self.cur.fetchall()
        print (res)

class Main(Checks):
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--username')
        parser.add_argument('--password')
        parser.add_argument('--address')
        parser.add_argument('--database')
        parser.add_argument('--hostname')
       
        subparsers = parser.add_subparsers()

        for name in dir(self):
            if not name.startswith("_"):
                p = subparsers.add_parser(name)
                method = getattr(self, name)
                argnames = inspect.getargspec(method).args[1:]
                for argname in argnames:
                    p.add_argument(argname)
                p.set_defaults(func=method, argnames=argnames)
        self.args = parser.parse_args()

    def db_connect(self):
        a = self.args
        username = a.username
        password = a.password
        address = a.address
        database = a.database
        self.db = cx_Oracle.connect("{0}/{1}@{2}:11521/{3}".format(
            username, password, address, database))
        self.cur = self.db.cursor()

    def db_close(self):
        self.db.close()

    def __call__(self):
        try:
            a = self.args
            callargs = [getattr(a, name) for name in a.argnames]
            self.db_connect()
            try:
                return self.args.func(*callargs)
            finally:
                self.db_close()
        except Exception as err:
            print("0")
            print(str(err))

if __name__ == "__main__":
    main = Main()
    main()
