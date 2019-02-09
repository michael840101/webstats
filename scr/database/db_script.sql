--PostgreSQL 9.5.14 on x86_64-pc-linux-gnu
--sql scripts for setting up the cc_dev database

--CREATE MAIN DOMAIN TABLE
CREATE  TABLE IF NOT EXISTS domains (
  domain character varying(128),
  url_total int,
  total_content bigint,
	url_increased int,
	url_decreased int,
  pg_rank int,
  content_rank int,
	--lang_code_0 character varying(36),
  --lang_code_1 character varying(36),
  stats_date timestamp
);

--CREATE  language and distribution table
CREATE  TABLE IF NOT EXISTS languages (
lang_code character varying(32),
language  character varying(128),
country  character varying(128),
population int
);



--CREATE AND PRE LOAD THE LOCATION OF IP ADDRESS TABLE
CREATE TABLE ip2location(
	ip_from bigint NOT NULL,
	ip_to bigint NOT NULL,
	country_code character(2) NOT NULL,
	country_name character varying(64) NOT NULL,
	region_name character varying(128) NOT NULL,
	city_name character varying(128) NOT NULL,
	latitude real NOT NULL,
	longitude real NOT NULL,
	zip_code character varying(30) NOT NULL,
  time_zone character varying(8) NOT NULL,
	CONSTRAINT ip2location_pkey PRIMARY KEY (ip_from, ip_to)
);

--sudo -u postgres psql postgres
--COPY ip2location FROM '/home/ubuntu/IP2LOCATION-LITE-DB11.CSV' WITH CSV QUOTE AS '"';



-----Plan B:  Create cc_url TABLE ---------
CREATE  TABLE IF NOT EXISTS cc_url (
  url  character varying(256),
  domain character varying(128),
  tld character varying(64),
  ip character varying(64),
  length int,
  date_time timestamp
);
