# v_SNAP_Date = YYYYMMDD
CREATE PROCEDURE `SP_DSA_CEMS`( v_SNAP_Date varchar(8) )
BEGIN

-- This SP used
	DECLARE v_SNAP_STR         datetime;
	DECLARE v_SNAP_END         datetime;

-- This SP Variable Setting
  set @v_SNAP_STR    = concat(str_to_date(v_SNAP_Date,'%Y%m%d'), ' 00:00:00');
  set @v_SNAP_END    = concat(str_to_date(v_SNAP_Date,'%Y%m%d'), ' 23:59:59');

  insert into CEMS.dsa_cems (
    cno
    ,polno
    ,m_time
    ,SOx_ppm
    ,NOx_ppm
    ,O2
    ,O2_std
    ,CMH
    )
select
	cno
    #,abbr
    ,polno
    ,m_time
    ,sum(SOx_ppm) SOx_ppm
    ,sum(NOx_ppm) NOx_ppm
    ,sum(O2) O2
    ,sum(CMH) CMH
from
(SELECT 
	cno
    #,abbr
    ,polno
    ,m_time
    ,case when item=222 then m_val end SOx_ppm
    ,NULL NOx_ppm
    ,NULL O2
    ,NULL CMH
FROM CEMS.ods_cems
where m_time between @v_SNAP_STR and @v_SNAP_END
and code2desc like '%正常%'

union all

SELECT 
	cno
    #,abbr
    ,polno
    ,m_time
    ,NULL SOx_ppm
    ,case when item=223 then m_val end NOx_ppm
    ,NULL O2
    ,NULL CMH
FROM CEMS.ods_cems
where m_time between @v_SNAP_STR and @v_SNAP_END
and code2desc like '%正常%'

union all

SELECT 
	cno
    #,abbr
    ,polno
    ,m_time
    ,NULL SOx_ppm
    ,NULL NOx_ppm
    ,case when item=236 then m_val end O2
    ,NULL CMH
FROM CEMS.ods_cems
where m_time between @v_SNAP_STR and @v_SNAP_END
and code2desc like '%正常%'

union all

SELECT 
	cno
    #,abbr
    ,polno
    ,m_time
    ,NULL SOx_ppm
    ,NULL NOx_ppm
    ,NULL O2
    ,case when item=248 then m_val end CMH
FROM CEMS.ods_cems
where m_time between @v_SNAP_STR and @v_SNAP_END
and code2desc like '%正常%'
) a
group by cno,polno,m_time;

  commit;

END

