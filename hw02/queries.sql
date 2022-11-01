hive> CREATE TABLE artists ( mbid string, artist_mb string, artist_lastfm string, 
    > country_mb string, country_lastfm string, tags_mb string, tags_lastfm string, 
    > listeners_lastfm int, scrobbles_lastfm int, ambiguous_artist boolean) 
    > ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' TBLPROPERTIES ("skip.header.line.count" = "1");
OK
Time taken: 1.453 seconds

hive> show tables;
OK
artists

hive> LOAD DATA LOCAL INPATH '/opt/hive/examples/files/artists.csv' OVERWRITE INTO TABLE artists;
Loading data to table default.artists
OK
Time taken: 1.837 seconds

hive> select artist_lastfm, scrobbles_lastfm from artists order by scrobbles_lastfm desc limit 0,1;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20221101210054_6499d437-4378-4c34-9a20-eb4135680922
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-11-01 21:00:56,071 Stage-1 map = 0%,  reduce = 0%
2022-11-01 21:00:57,075 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local530765512_0002
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 804429680 HDFS Write: 402206648 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
The Beatles	517126254
Time taken: 2.415 seconds, Fetched: 1 row(s)

hive> SELECT tag, COUNT(*) AS tag_count FROM artists
    > LATERAL VIEW EXPLODE(SPLIT(tags_lastfm, ';')) t AS tag
    > WHERE tag != '' GROUP BY tag ORDER BY tag_count DESC LIMIT 1;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20221101210341_8a436434-2c5a-48ac-90f5-0ddd6cb13930
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-11-01 21:03:43,257 Stage-1 map = 0%,  reduce = 0%
2022-11-01 21:03:48,262 Stage-1 map = 100%,  reduce = 0%
2022-11-01 21:03:49,266 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local562571768_0003
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-11-01 21:03:50,445 Stage-2 map = 100%,  reduce = 100%
Ended Job = job_local932424583_0004
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 1206644520 HDFS Write: 402206648 SUCCESS
Stage-Stage-2:  HDFS Read: 1206644520 HDFS Write: 402206648 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
 seen live	81278
Time taken: 8.502 seconds, Fetched: 1 row(s)

hive> with t as 
    > (
    > select  TagName, artist_lastfm,  listeners_lastfm  from artists l
    > LATERAL VIEW explode(SPLIT(tags_lastfm, ';')) tags AS tagname
    > where tagname != ''
    > ),
    > top as (
    > select tagname, count(*) cnt from t group by tagname order by cnt desc limit 10
    > ) 
    > select * 
    > from 
    > (
    >     select 
    >         t.tagname
    >         , t.artist_lastfm
    >         , row_number() over (partition by t.tagname order by listeners_lastfm desc) rn
    >         , listeners_lastfm
    >     from t
    >     join top on t.tagname = top.tagname 
    > ) t0
    > where rn = 1;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20221101212512_2753ab93-b3b6-4062-a1e2-e753b35df714
Total jobs = 6
Launching Job 1 out of 6
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-11-01 21:25:14,270 Stage-3 map = 0%,  reduce = 0%
2022-11-01 21:25:20,274 Stage-3 map = 100%,  reduce = 100%
Ended Job = job_local812612034_0009
Launching Job 2 out of 6
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-11-01 21:25:21,427 Stage-4 map = 100%,  reduce = 100%
Ended Job = job_local1087770307_0010
Stage-8 is selected by condition resolver.
Stage-9 is filtered out by condition resolver.
Stage-1 is filtered out by condition resolver.
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-2.7.4/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Execution log at: /tmp/root/root_20221101212512_2753ab93-b3b6-4062-a1e2-e753b35df714.log
2022-11-01 21:25:25	Starting to launch local task to process map join;	maximum memory = 477626368
2022-11-01 21:25:26	Dump the side-table for tag: 1 with group count: 10 into file: file:/tmp/root/17351978-b635-40d3-aaad-fb62634f53bc/hive_2022-11-01_21-25-12_925_9022999161691001491-1/-local-10006/HashTable-Stage-5/MapJoin-mapfile21--.hashtable
2022-11-01 21:25:26	Uploaded 1 File to: file:/tmp/root/17351978-b635-40d3-aaad-fb62634f53bc/hive_2022-11-01_21-25-12_925_9022999161691001491-1/-local-10006/HashTable-Stage-5/MapJoin-mapfile21--.hashtable (546 bytes)
2022-11-01 21:25:26	End of local task; Time Taken: 0.545 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 4 out of 6
Number of reduce tasks is set to 0 since there's no reduce operator
Job running in-process (local Hadoop)
2022-11-01 21:25:28,235 Stage-5 map = 0%,  reduce = 0%
2022-11-01 21:25:33,238 Stage-5 map = 67%,  reduce = 0%
2022-11-01 21:25:34,240 Stage-5 map = 100%,  reduce = 0%
Ended Job = job_local1420654844_0011
Launching Job 5 out of 6
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-11-01 21:25:35,393 Stage-2 map = 0%,  reduce = 0%
2022-11-01 21:25:36,395 Stage-2 map = 100%,  reduce = 100%
Ended Job = job_local163851628_0012
MapReduce Jobs Launched: 
Stage-Stage-3:  HDFS Read: 2413289040 HDFS Write: 402206648 SUCCESS
Stage-Stage-4:  HDFS Read: 2413289040 HDFS Write: 402206648 SUCCESS
Stage-Stage-5:  HDFS Read: 1407751940 HDFS Write: 201103324 SUCCESS
Stage-Stage-2:  HDFS Read: 2815503880 HDFS Write: 402206648 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
 All	Jason Derülo	1	1872933
 alternative	Coldplay	1	5381567
 electronic	Coldplay	1	5381567
 experimental	Radiohead	1	4732528
 female vocalists	Rihanna	1	4558193
 indie	Coldplay	1	5381567
 pop	Coldplay	1	5381567
 rock	Radiohead	1	4732528
 seen live	Coldplay	1	5381567
 under 2000 listeners	Diddy - Dirty Money	1	503188
Time taken: 23.473 seconds, Fetched: 10 row(s)

--Топ-10 исполнителей по популярности
hive> SELECT artist_lastfm, listeners_lastfm FROM artists ORDER BY listeners_lastfm DESC LIMIT 10;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20221101212817_184e40b2-f650-4995-887e-320ba2e81918
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-11-01 21:28:18,943 Stage-1 map = 0%,  reduce = 0%
2022-11-01 21:28:19,944 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local275124392_0013
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 3217718720 HDFS Write: 402206648 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Coldplay	5381567
Radiohead	4732528
Red Hot Chili Peppers	4620835
Rihanna	4558193
Eminem	4517997
The Killers	4428868
Kanye West	4390502
Nirvana	4272894
Muse	4089612
Queen	4023379
Time taken: 2.268 seconds, Fetched: 10 row(s)

