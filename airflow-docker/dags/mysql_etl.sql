SET SQL_SAFE_UPDATES=0;
call CEMS.SP_DSA_CEMS('{{ds_nodash }}0000000',{{ ds_nodash  }}) ;
call CEMS.SP_D_FACTORY('{{ds_nodash }}0000000',{{ ds_nodash  }}) ;
call CEMS.SP_F_EMISSION('{{ds_nodash }}0000000',{{ ds_nodash  }}) ;
SET SQL_SAFE_UPDATES=1;