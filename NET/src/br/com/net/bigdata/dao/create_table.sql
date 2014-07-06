/*CREATE TABLE CDR*/
-- Table: hawqprod.tb_cdr

-- DROP TABLE hawqprod.tb_cdr;

CREATE TABLE hawqprod.tb_cdr
(
  starttime timestamp with time zone,
  endtime timestamp with time zone,
  subscriberip inet,
  username text,
  usercpf text,
  contextname text,
  macaddress macaddr,
  nasipaddress inet,
  netdevicemode text,
  netdeviceos text,
  netdevicebrowser text,
  referencedate timestamp without time zone,
  referencefile text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, COMPRESSTYPE=zlib, 
  OIDS=FALSE
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(starttime) 
          (
          START ('2014-06-20 00:00:00+08'::timestamp with time zone) END ('2014-06-27 00:00:00+08'::timestamp with time zone) INCLUSIVE EVERY ('1 day'::interval) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          DEFAULT PARTITION outlying  WITH (appendonly=true, compresslevel=5, compresstype=zlib)
          )
;
ALTER TABLE hawqprod.tb_cdr
  OWNER TO gpadmin;


/* criar uma partição com o nome "25" no periodo indicado e outra default: table tb_cdr_1_prt_25 
 * partição de 28/06/2014 - 28/07/2014
 *  */
ALTER TABLE hawqprod.tb_cdr SPLIT DEFAULT PARTITION
START ('2014-06-28 00:00:00+08'::timestamp with time zone) END ('2014-07-28 00:00:00+08'::timestamp with time zone)
INTO (PARTITION "25", PARTITION default)

/*consultar partições*/
select * from pg_partitions where tablename = 'tb_cdr' and schemaname = 'hawqprod' order by partitionrangestart
  
/*Droppar partições*/
ALTER TABLE  hawqprod.tb_cdr drop partition "20140629"

  
/*CREATE TABLE CGNAT*/
-- Table: hawqprod.tb_cgnat

-- DROP TABLE hawqprod.tb_cgnat;

CREATE TABLE hawqprod.tb_cgnat
(
  starttime timestamp with time zone,
  endtime timestamp with time zone,
  originip inet,
  originport integer,
  translateip inet,
  translateport integer,
  referencedate timestamp without time zone,
  referencefile text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, COMPRESSTYPE=zlib, 
  OIDS=FALSE
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(starttime) 
          (
          START ('2014-06-20 00:00:00+08'::timestamp with time zone) END ('2014-06-27 00:00:00+08'::timestamp with time zone) INCLUSIVE EVERY ('1 day'::interval) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          DEFAULT PARTITION outlying  WITH (appendonly=true, compresslevel=5, compresstype=zlib)
          )
;
ALTER TABLE hawqprod.tb_cgnat
  OWNER TO gpadmin;


/*CREATE TABLE DHCP*/
-- Table: hawqprod.tb_dhcp

-- DROP TABLE hawqprod.tb_dhcp;

CREATE TABLE hawqprod.tb_dhcp
(
  starttime timestamp with time zone,
  endtime timestamp with time zone,
  ipaddress inet,
  hwaddress macaddr,
  action text,
  protocol text,
  referencedate timestamp without time zone,
  referencefile text
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, COMPRESSTYPE=zlib, 
  OIDS=FALSE
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(starttime) 
          (
          START ('2014-06-20 00:00:00+08'::timestamp with time zone) END ('2014-06-27 00:00:00+08'::timestamp with time zone) INCLUSIVE EVERY ('1 day'::interval) WITH (appendonly=true, compresslevel=5, compresstype=zlib), 
          DEFAULT PARTITION outlying  WITH (appendonly=true, compresslevel=5, compresstype=zlib)
          )
;
ALTER TABLE hawqprod.tb_dhcp
  OWNER TO gpadmin;

  
/*CREATE TABLE TBLOG*/
-- Table: hawqaux.tb_log

-- DROP TABLE hawqaux.tb_log;

CREATE TABLE hawqaux.tb_log
(
  importdate timestamp without time zone,
  filename text,
  status smallint,
  descriptionerror text
)
WITH (APPENDONLY=true, 
  OIDS=FALSE
)
DISTRIBUTED RANDOMLY;
ALTER TABLE hawqaux.tb_log
  OWNER TO gpadmin;
  