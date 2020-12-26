/**
 * Data base objects
 * this scripts is supposed to be applied in PostgreSQL.
 * @author: pavel.filkovskiy
 */
drop table if exists "Connection";
CREATE TABLE public."Connection" (
	"ID" int4 NOT null,
	"DATETIME" timestamp NOT null,
	"Rule" varchar(2000) null,
	"Service" varchar(255) null,
	"User" varchar(255) null,
	"Protocol" varchar(100) null,
	"SourceHost" varchar(255) null,
	"SourceIp" varchar(16) null,
	"SourcePort" int4 null,
	"DestinationHost" varchar(255) null,
	"DestinationIp" varchar(16) null,
	"DestinationPort" int4 null,
	"Duration" int4 null,
	"Bytes.Transmitted" bigint null,
	"Bytes.Accepted" bigint null,
	"Bytes.Total" bigint null,
	"Packets.Transmitted" int4 null,
	"Packets.Accepted" int4 null,
	"Packets.Total" int4 null,
	
	-- CONSTRAINT "connection_p_key" PRIMARY KEY ("ID", "DATETIME")
	--- optional, in case if we want to set a custom name  
	--- for this constraint, say "connection_prim_key".
	--- by default it would be "<table_name>_pkey" instead:	
	
	--- otherwise just leave 
	--- PRIMARY KEY ("ID", "DATETIME")
	--- and constraint name will be created automatically.
	PRIMARY KEY ("ID", "DATETIME")
);


/**
 * Daily Totals by users aggregated by dates:
 */
create or replace view public."vDailyTotals"
as select 
	c."DATETIME"::date as "Date", c."User",
	--
	cast(sum(c."Bytes.Transmitted") / 1024 / 1024 as decimal(15, 2)) as "MB.Transmitted",
	cast(sum(c."Bytes.Accepted") / 1024 / 1024 as decimal(15, 2)) as "MB.Accepted",
	cast(sum(c."Bytes.Total") / 1024 / 1024 as decimal(15, 2)) as "MB.Total",
	--
	sum(c."Packets.Transmitted") as "Packets.Transmitted",
	sum(c."Packets.Accepted") as "Packets.Accepted",
	sum(c."Packets.Total") as "Packets.Total"
from public."Connection" c
group by c."DATETIME"::date, "User";

/**
 * Daily Totals by users at the exact date (YYYY-MM-DD):
 */
create or replace function public."getDailyTotals"("date" date)
	returns table (
   		"Date" date, "User" varchar,
		"MB.Transmitted" decimal(15, 2),
   		"MB.Accepted" decimal(15, 2),
   		"MB.Total" decimal(15, 2),
   		"Packets.Transmitted" bigint,
   		"Packets.Accepted" bigint,
   		"Packets.Total" bigint)
as $$
	select 
		"DATETIME"::date as "Date", "User",
		--
		cast(sum("Bytes.Transmitted") / 1024 / 1024 as decimal(15, 2)) as "MB.Transmitted",
		cast(sum("Bytes.Accepted") / 1024 / 1024 as decimal(15, 2)) as "MB.Accepted",
		cast(sum("Bytes.Total") / 1024 / 1024 as decimal(15, 2)) as "MB.Total",
		--
		sum("Packets.Transmitted")::bigint as "Packets.Transmitted",
		sum("Packets.Accepted")::bigint as "Packets.Accepted",
		sum("Packets.Total")::bigint as "Packets.Total"
	from public."Connection" 
	where 1=1
	and "DATETIME"::date = "date"
	group by "Date", "User"
$$ language 'sql' volatile;


/**
 * User's totas at the exact date aggregated by hour:
 */
create or replace function public."getHourlyTotals"("date" date, "user" varchar)
	returns table (
   		"Hour" smallint,
		"Date" date, "User" varchar,
		"MB.Transmitted" decimal(15, 2),
   		"MB.Accepted" decimal(15, 2),
   		"MB.Total" decimal(15, 2),
   		"Packets.Transmitted" bigint,
   		"Packets.Accepted" bigint,
   		"Packets.Total" bigint)
as $$
	select 
		extract ('hour' from "DATETIME")::smallint as "Hour",
		"DATETIME"::date as "Date", "User",
		--
		cast(sum("Bytes.Transmitted") / 1024 / 1024 as decimal(15, 2)) as "MB.Transmitted",
		cast(sum("Bytes.Accepted") / 1024 / 1024 as decimal(15, 2)) as "MB.Accepted",
		cast(sum("Bytes.Total") / 1024 / 1024 as decimal(15, 2)) as "MB.Total",
		--
		sum("Packets.Transmitted")::bigint as "Packets.Transmitted",
		sum("Packets.Accepted")::bigint as "Packets.Accepted",
		sum("Packets.Total")::bigint as "Packets.Total"
	from public."Connection" 
	where 1=1
	and "DATETIME"::date = "date"
	and case
		when "user" is null then true
		else "User" = "user"
	end
	group by "Date", "User", "Hour"
	order by "User", "Hour"
$$ language 'sql' volatile;


/**
 * User's totas at the exact date aggregated by custom time interval:
 * -- for more info see https://www.postgresql.org/docs/9.1/functions-datetime.html
 */
create or replace function public."getIntervalTotals"("interval" interval, "date" date, "user" varchar)
	returns table (
   		"Time" time, "Date" date, "User" varchar,
   		--
		"MB.Transmitted" decimal(15, 2),
   		"MB.Accepted" decimal(15, 2),
   		"MB.Total" decimal(15, 2),
   		--
   		"Packets.Transmitted" bigint,
   		"Packets.Accepted" bigint,
   		"Packets.Total" bigint)
as $$
	with s as (
		select 
			i::time as "Time",
			i::date as "Date",
			i as "StartTime",
			i + "interval"::interval - interval '1sec' as "EndTime"
		from generate_series(
			"date"::timestamp, 
			"date"::timestamp + interval '1day' - interval '1sec',
			"interval"::interval
		) as g(i)
	) select 
		s."Time", s."Date", c."User",
		--
		cast(sum(c."Bytes.Transmitted") / 1024 / 1024 as decimal(15, 2)) as "MB.Transmitted",
		cast(sum(c."Bytes.Accepted") / 1024 / 1024 as decimal(15, 2)) as "MB.Accepted",
		cast(sum(c."Bytes.Total") / 1024 / 1024 as decimal(15, 2)) as "MB.Total",
		--
		sum(c."Packets.Transmitted")::bigint as "Packets.Transmitted",
		sum(c."Packets.Accepted")::bigint as "Packets.Accepted",
		sum(c."Packets.Total")::bigint as "Packets.Total"
	from s
	inner join public."Connection" c
		on c."DATETIME"::date = s."Date"::date
		and c."DATETIME" between s."StartTime" and s."EndTime"
		and case
			when "user" is null then true
			else "User" = "user"
		end
	group by
		s."Date", s."Time", c."User"
	-- order by s."Time"
$$ language 'sql' volatile;
