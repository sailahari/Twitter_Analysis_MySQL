-- loading data

CREATE DATABASE `TwitterResearch_Fall2014`;

Create Table EarnRelDate (

	ticker VARCHAR(8),
	symbol VARCHAR(8),
	cname VARCHAR(40),
	pends DATE,
	pdicity VARCHAR(8),
	anndats DATE,
	anntimes decimal,
	actualdate DATE,
	earnrelease_date DATE,
	earnrelease_time TIME
);

LOAD DATA INFILE 'C:/earnings_processed_Sept30_2014_1110pm.csv'  INTO TABLE  EarnRelDate
CHARACTER SET utf8
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r'
IGNORE 1 LINES
(ticker, symbol, cname, pends, pdicity, anndats, anntimes, actualdate,  earnrelease_date,  earnrelease_time);

Select count(*) from EarnRelDate;

Select * from EarnRelDate where symbol is not null and symbol != "" order by earnrelease_date, earnrelease_time;

Create Table EarnRelDate2 (

	ticker VARCHAR(8),
	symbol VARCHAR(8),
	cname VARCHAR(40),
	pends DATE,
	pdicity VARCHAR(8),
	anndats DATE,
	anntimes decimal,
	actualdate DATE,
	earnrelease_date DATE,
	earnrelease_time TIME,
	PRIMARY KEY (ticker, earnrelease_date,earnrelease_time, pdicity) 	
) ENGINE=InnoDB;

Insert into EarnRelDate2 (ticker, earnrelease_date,earnrelease_time, pdicity )
Select distinct ticker, earnrelease_date,earnrelease_time, pdicity from EarnRelDate;

Select count(*) from EarnRelDate2;

Create Table EarnRelDate3 (

	ticker VARCHAR(8),
	symbol VARCHAR(8),
	earnrelease_date DATE,
	earnrelease_time TIME,
	PRIMARY KEY (ticker, earnrelease_date,earnrelease_time) 	
) ENGINE=InnoDB;

Insert into EarnRelDate3 (ticker, earnrelease_date,earnrelease_time )
Select distinct ticker, earnrelease_date,earnrelease_time from EarnRelDate;

Select count(*) from EarnRelDate3;

Create Table Tweets (

	smblid VARCHAR(10),
	symbol VARCHAR(8),
	periodnum INT,
	periodnum_inday INT, 
	volumestart INT,
	volumeend INT,
	twittermentions INT,
	twitterpermin DECIMAL(10,2), 
	averagefollowers DECIMAL(10,2),
	datestart DATE,
	timestart TIME,
	dateend DATE, 
	timeend TIME	
);

LOAD DATA INFILE 'C:/TwitterYahoo_INTERM1_Oct4_2013.csv'  INTO TABLE  Tweets
CHARACTER SET utf8
FIELDS TERMINATED BY ','
 OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(smblid, symbol, periodnum, periodnum_inday, @volumestart, @volumeend, @twittermentions, @twitterpersec, @averagefollowers, datestart, timestart,dateend, timeend)
SET volumestart = IF(@volumestart='',0,@volumestart),
volumeend = IF(@volumeend='',0,@volumeend),
twittermentions = IF(@twittermentions='',null,@twittermentions),
twitterpermin = IF(@twitterpersec='',null,60*@twitterpersec),
averagefollowers = IF(@averagefollowers='',null,@averagefollowers);

select count(*) from Tweets;
select max(twitterpermin) from Tweets;
select avg(averagefollowers) from Tweets;
select count(*) from Tweets where smblid is not null;

Create Table Tweets2 (
	smblid VARCHAR(10),
	periodnum INT,
	periodnum_inday INT, 
	datestart DATE,
	timestart TIME,
	dateend DATE, 
	timeend TIME,	
	PRIMARY KEY (smblid, periodnum), 	
	INDEX tstart (smblid, datestart, timestart),
	INDEX tend (smblid, dateend, timeend)
);

Insert into Tweets2 (smblid,periodnum, periodnum_inday, datestart, timestart, dateend, timeend) 
Select distinct smblid,periodnum, periodnum_inday, datestart, timestart, dateend, timeend from Tweets;

select count(*) from Tweets2;

Select e.ticker, e.earnrelease_date, e.earnrelease_time,
t.smblid, t.periodnum, t.periodnum_inday, t.datestart, t.timestart, t.dateend, t.timeend 
 from Tweets2 t, EarnRelDate3 e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;

explain Select e.ticker, e.earnrelease_date, e.earnrelease_time,
t.smblid, t.periodnum, t.periodnum_inday, t.datestart, t.timestart, t.dateend, t.timeend 
 from Tweets2 t, EarnRelDate3 e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;

Select count(*)
 from Tweets2 t, EarnRelDate3 e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;

Create Table EarnRelMatched (
	ticker VARCHAR(10),
	earnrelease_date DATE,
	earnrelease_time TIME,
	smblid VARCHAR(10),
	periodnum INT, 
	periodnum_inday INT,
	datestart DATE, 
	timestart TIME,
	dateend DATE, 
	timeend TIME,
	PRIMARY KEY (smblid, periodnum) 	
);

explain Select e.ticker, e.earnrelease_date, e.earnrelease_time,
t.smblid, t.periodnum, t.periodnum_inday, t.timestart, t.timeend 
 from Tweets t, EarnRelDate e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid; 

explain Select count(*)
 from Tweets t, EarnRelDate e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;

INSERT into EarnRelMatched
Select e.ticker, e.earnrelease_date, e.earnrelease_time,
t.smblid, t.periodnum, t.periodnum_inday, t.datestart, t.timestart, t.dateend, t.timeend 
 from Tweets2 t, EarnRelDate3 e
where e.earnrelease_date = t.datestart
and e.earnrelease_time BETWEEN t.timestart AND t.timeend
and e.ticker = t.smblid;

Select count(*) from EarnRelMatched;

create table datetable (date1 DATE, weekname varchar(10), PRIMARY KEY(date1));

LOAD DATA INFILE 'C:/datetable.csv' INTO TABLE datetable FIELDS TERMINATED BY ',' ENCLOSED BY '"'LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from datetable;

Create Table EarnRelMatched1 (
	ticker VARCHAR(10),
	earnrelease_date DATE,
	earnrelease_time TIME,
	smblid VARCHAR(10),
	periodnum INT, 
	periodnum_inday INT,
	datestart DATE, 
	timestart TIME,
	dateend DATE, 
	timeend TIME,
   earnrelease_weekname varchar(10),
	PRIMARY KEY (smblid, periodnum) 	
);

insert into earnrelmatched1 select e.ticker,e.earnrelease_date,e.earnrelease_time, e.smblid,e.periodnum,e.periodnum_inday,
e.datestart,e.timestart,e.dateend,e.timeend,d.weekname from earnrelmatched e,datetable d where e.earnrelease_date=d.date1;

select count(*) from earnrelmatched1;

Create Table Tweets3 (
	smblid VARCHAR(10),
	periodnum INT,
	periodnum_inday INT,
    volumestart INT,
	volumeend INT,
    twitterpermin DECIMAL(10,2), 
	datestart DATE,
	timestart TIME,
	dateend DATE, 
	timeend TIME,	
    Datestartname varchar(10),
	PRIMARY KEY (smblid, periodnum), 	
	INDEX tstart (smblid, datestart, timestart),
	INDEX tend (smblid, dateend, timeend)
);

insert into tweets3 (smblid,periodnum,periodnum_inday,volumestart,volumeend,twitterpermin,datestart,timestart,dateend,timeend,Datestartname) select t.smblid, t.periodnum, t.periodnum_inday,t.volumestart,t.volumeend,t.twitterpermin, t.datestart,t.timestart,
t.dateend, t.timeend,d.weekname from tweets t,datetable d where t.datestart=d.date1;

select count(*) from tweets3;

-- question 1

select count(*) from (select smblid,count(distinct datestart) as c from tweets where twitterpermin!=0 
group by smblid having c>=30)t;

-- question 2a

select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.twitterpermin) from EarnRelMatched e, tweets t where t.smblid=e.ticker
and t.datestart=e.earnrelease_date
 and t.periodnum_inday BETWEEN e.periodnum_inday+1 and e.periodnum_inday+4 group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;
 
 -- question 2b

select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.twitterpermin) from EarnRelMatched e, tweets t where t.smblid=e.ticker
and t.datestart=e.earnrelease_date
 and t.periodnum_inday BETWEEN e.periodnum_inday+1 and e.periodnum_inday+12 group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;
 
 -- question 2c
 
 select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.twitterpermin) from EarnRelMatched e, tweets t 
where t.smblid=e.ticker and TIMESTAMP(t.datestart,t.timestart) between TIMESTAMP(e.earnrelease_date,e.timestart) and
 addtime((TIMESTAMP(e.earnrelease_date,e.timestart)),'24:00:00') 
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;

-- question 2d

select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.twitterpermin),addtime((TIMESTAMP(e.earnrelease_date,e.timestart)),'168:00:00') as timeperiod_endtime from EarnRelMatched e, tweets t 
where t.smblid=e.ticker and TIMESTAMP(t.datestart,t.timestart) between TIMESTAMP(e.earnrelease_date,e.earnrelease_time) and
 addtime((TIMESTAMP(e.earnrelease_date,e.earnrelease_time)),'168:00:00') 
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;

-- question 2e

select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.twitterpermin),t.Datestartname from EarnRelMatched1 e left outer join tweets3 t on t.smblid=e.ticker and t.datestart>=2012-05-01 and t.datestart<=e.earnrelease_date-7 and t.Datestartname=e.earnrelease_weekname
and t.timestart between subtime(e.timeend,'00:10:00') and addtime(e.timeend,'00:20:00')
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;
 
-- question 3a

select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.volumeend) from EarnRelMatched e left outer join tweets t on t.smblid=e.ticker
and t.datestart=e.earnrelease_date
 and t.periodnum_inday BETWEEN e.periodnum_inday+1 and e.periodnum_inday+4 group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;
 
-- question 3b

 select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.volumeend) from EarnRelMatched e left outer join tweets t on t.smblid=e.ticker
and t.datestart=e.earnrelease_date
 and t.periodnum_inday BETWEEN e.periodnum_inday+1 and e.periodnum_inday+12 group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;
 
 -- question 3c
 
 select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.volumeend) from EarnRelMatched e left outer join tweets t 
on t.smblid=e.ticker and TIMESTAMP(t.datestart,t.timestart) between TIMESTAMP(e.earnrelease_date,e.earnrelease_time) and
 addtime((TIMESTAMP(e.earnrelease_date,e.earnrelease_time)),'24:00:00') 
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;

-- question 3d

select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.volumeend),addtime((TIMESTAMP(e.earnrelease_date,e.timestart)),'168:00:00') as timeperiod_endtime from EarnRelMatched e, tweets t 
where t.smblid=e.ticker and TIMESTAMP(t.datestart,t.timestart) between TIMESTAMP(e.earnrelease_date,e.earnrelease_time) and
 addtime((TIMESTAMP(e.earnrelease_date,e.earnrelease_time)),'168:00:00') 
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;

-- question 3e

select e.ticker,e.earnrelease_date,e.earnrelease_time,e.timestart,avg(t.volumeend),t.Datestartname from EarnRelMatched1 e left outer join tweets3 t on t.smblid=e.ticker and t.datestart>=2012-05-01 and t.datestart<=e.earnrelease_date-7 and t.Datestartname=e.earnrelease_weekname
and t.timestart between subtime(e.timeend,'00:10:00') and addtime(e.timeend,'00:20:00')
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart;

-- queries for analysis

select ticker,earnrelease_date,earnrelease_time from  EarnRelMatched where (ticker,earnrelease_date,earnrelease_time) not in 
(select e.ticker,e.earnrelease_date,e.earnrelease_time from EarnRelMatched e, tweets2 t where t.smblid=e.ticker 
and t.datestart=e.earnrelease_date and t.periodnum_inday BETWEEN e.periodnum_inday+1 and e.periodnum_inday+4 
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart);

select ticker,earnrelease_date,earnrelease_time from  EarnRelMatched where (ticker,earnrelease_date,earnrelease_time) not in 
(select e.ticker,e.earnrelease_date,e.earnrelease_time from EarnRelMatched e, tweets2 t where t.smblid=e.ticker 
and t.datestart=e.earnrelease_date and t.periodnum_inday BETWEEN e.periodnum_inday+1 and e.periodnum_inday+12 
group by e.ticker,e.earnrelease_date,earnrelease_time,e.timestart)

