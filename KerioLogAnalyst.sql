/**
 * Start of the traffic:
 */
select 
	min("DATETIME")
	-- min("DestinationPort") 
from "Connection" c where true 
and "SourceIp" = '192.168.x.x' 
and "DATETIME"::date = '20201223'
and ("DestinationHost" like '%roblox%' or "DestinationHost" like '%rbx%')  
-- and "DestinationIp" like '128%'
-- and "DestinationPort" < 50000 
-- and "Protocol" = 'UDP'


/**
 * Daily Totals in MB:
 */
select * from public."vDailyTotals" where "Date" between '2020-10-20' and '2020-10-28'
select * from "getDailyTotals"("date" => '2020-12-23')
select * from "getHourlyTotals"("date" => '2020-12-23', "user" => 'Veronika')


select * from "getIntervalTotals"(
	"interval" => '15min', 
	"date" => '2020-12-23', 
	"user" => 'Veronika') 
where 1=1
and "Time" between '14:00:00' and '17:00:59'


/**
 * Quick log by Time:
 */
select 
	"DATETIME", "ID", 
	"SourceHost", "SourceIp",
	"DestinationHost", "DestinationIp" , "DestinationPort", 
	"Protocol", "Rule" 
from "Connection" c 
where true 
-- and "SourceIp" = '192.168.x.x'
and "User" = 'Veronika'
and "DATETIME"::date = '20201223'
and "DATETIME"::time between '18:49:00' and '18:51:59'
-- and "DestinationPort" in (5222, 5223, 5228) and "Protocol" = 'UDP'
-- and "Rule" in ('TEST')
-- and "DestinationIp" = '40.87.147.37'
-- and "DestinationHost" like '%amazon%'
order by "DATETIME"::time,
	split_part("DestinationIp", '.', 1)::smallint,
	split_part("DestinationIp", '.', 2)::smallint,
	split_part("DestinationIp", '.', 3)::smallint,
	split_part("DestinationIp", '.', 4)::smallint

/**
 * Totals by user by Destination Host
 */ 
select 
	"DestinationHost", "DestinationIp" ,
	--
	cast(sum(c."Bytes.Transmitted") / 1024 / 1024 as decimal(15, 2)) as "MB.Transmitted",
	cast(sum(c."Bytes.Accepted") / 1024 / 1024 as decimal(15, 2)) as "MB.Accepted",
	cast(sum(c."Bytes.Total") / 1024 / 1024 as decimal(15, 2)) as "MB.Total",
	--
	sum(c."Packets.Transmitted")::bigint as "Packets.Transmitted",
	sum(c."Packets.Accepted")::bigint as "Packets.Accepted",
	sum(c."Packets.Total")::bigint as "Packets.Total"
from "Connection" c
where 1=1
and "User" = 'Veronika'
and "DATETIME"::date = '20201223'
and "DATETIME"::time between '10:15:00' and '16:45:59'
group by "DestinationHost", "DestinationIp"
having sum(c."Bytes.Total") > 10e6
order by 
	split_part("DestinationIp", '.', 1)::smallint,
	split_part("DestinationIp", '.', 2)::smallint,
	split_part("DestinationIp", '.', 3)::smallint,
	split_part("DestinationIp", '.', 4)::smallint,
	sum(c."Bytes.Total") desc