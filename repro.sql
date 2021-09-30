-- -c shared_buffers=256kB -c bgwriter_delay=10

create extension chaos;
create extension pg_buffercache;
create extension pg_prewarm;

drop table if exists t1, t2;
checkpoint;
vacuum pg_class;

-- fill the bufferpool up with dirty data from t1 (relfilenode 2000000 gen 1)
select clobber_next_oid(200000);
create table t1 as select 42 i from generate_series(1, 10000);
select pg_prewarm('t1'); -- fill buffer pool with t1
update t1 set i = i; -- dirty all t1 buffers so bgwriter writes some
select relfilenode, count(*) as t1_dirty_buffers_before_sleep
  from pg_buffercache
 where relfilenode = 't1'::regclass and isdirty
 group by 1;
select pg_sleep(5); -- give bgwriter some time
select relfilenode, count(*) as t1_dirty_buffers_after_sleep
  from pg_buffercache
 where relfilenode = 't1'::regclass and isdirty
 group by 1;

-- expunge t1 from buffers, fs and cat index so we can steal its relfilenode
drop table t1;
checkpoint;
vacuum pg_class;

-- fill the bufferpool up with dirty data from t2 = relfilenode 2000000 gen 2
select clobber_next_oid(200000);
create table t2 as select 0::int i from generate_series(1, 10000);
select pg_prewarm('t2'); -- fill buffer pool with t2
update t2 set i = 1 where i = 0; -- dirty all buffers so bgwriter writes some
select relfilenode, count(*) as t2_dirty_buffers_before_sleep
  from pg_buffercache
 where relfilenode = 't2'::regclass and isdirty
 group by 1;
--select sum(i) as t2_sum_before_corruption from t2;
select pg_sleep(10); -- give bgwriter some time
select relfilenode, count(*) as t2_dirty_buffers_after_sleep
 from pg_buffercache
 where relfilenode = 't2'::regclass and isdirty
 group by 1;

-- obvserve eaten data
select pg_prewarm('pg_attribute'); -- evict all clean t2 buffers
select relfilenode, count(*) as t2_buffers
 from pg_buffercache
 where relfilenode = 't2'::regclass
 group by 1;
select sum(i) as t2_sum_after_corruption from t2;


