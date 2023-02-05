# v_SNAP_Date = YYYYMMDD
CREATE PROCEDURE `SP_F_EMISSION`(v_SNAP_Date varchar(8) )
BEGIN

-- This SP used
	DECLARE v_SNAP_STR         datetime;
	DECLARE v_SNAP_END         datetime;

-- This SP Variable Setting
  set @v_SNAP_STR    = concat(str_to_date(v_SNAP_Date,'%Y%m%d'), ' 00:00:00');
  set @v_SNAP_END    = concat(str_to_date(v_SNAP_Date,'%Y%m%d'), ' 23:59:59');


  insert into CEMS.f_emission (
    cno
    ,abbr
    ,m_time
    ,E_SOx
    ,E_NOx
    )
	select 
		a.cno 
        ,b.abbr
        ,a.m_time
        ,E_SOx
        ,E_NOx
	from
    (select 
		a.cno
		,a.m_time
		,ROUND(sum(2.86*a.SOx_ppm*a.CMH/1000000),5) E_SOx
		,ROUND(sum(2.05*a.NOx_ppm*a.CMH/1000000),5) E_NOx
	from CEMS.dsa_cems as a
	where a.m_time between @v_SNAP_STR and @v_SNAP_END
	group by a.cno,a.m_time) a
	left join CEMS.d_factory b
	on a.cno = b.cno;

  commit;

END
