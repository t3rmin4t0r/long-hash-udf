

set hive.llap.execution.mode=none;
add jar /tmp//hive-long-hasher-1.0-SNAPSHOT.jar;

create temporary function longhash as 'org.notmysock.hive.udf.GenericLongHashUdf';
create temporary function long2hash as 'org.notmysock.hive.udf.GenericLong2HashUdf';


select longhash(1), longhash(2), longhash(3);
select long2hash(0,1), long2hash(0,2), long2hash(0,3);

