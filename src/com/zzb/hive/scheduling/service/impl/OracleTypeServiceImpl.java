package com.zzb.hive.scheduling.service.impl;

import com.zzb.hive.scheduling.service.TypeService;
import com.zzb.hive.scheduling.typeimpl.NumberType;
import com.zzb.hive.scheduling.typeimpl.VarChar2Type;

public class OracleTypeServiceImpl implements TypeService {
    @Override
    public String getType(String type_name) {
        String type = null ;
        if(type_name.indexOf("varchar") != -1){
            type = new VarChar2Type().getTypeName(type_name);
        }else{
            type = new NumberType().getTypeName(type_name);
        }
        return type;
    }
}
