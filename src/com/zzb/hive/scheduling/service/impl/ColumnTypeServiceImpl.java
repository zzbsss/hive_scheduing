package com.zzb.hive.scheduling.service.impl;


import com.zzb.hive.scheduling.enums.DbEnum;
import com.zzb.hive.scheduling.factory.MysqlTypeFactory;
import com.zzb.hive.scheduling.factory.OracleTypeFactory;
import com.zzb.hive.scheduling.provider.TypeProvider;
import com.zzb.hive.scheduling.service.ColumnTypeService;
import com.zzb.hive.scheduling.service.TypeService;
import com.zzb.hive.scheduling.vo.Column;

public class ColumnTypeServiceImpl implements ColumnTypeService {
    @Override
    public String getColumnType(Column column, String dbType) {
        String resultType = null;
        TypeService typeService = null;
        String type = column.getType();
        TypeProvider otf;
        if(DbEnum.oracle.toString().equals(dbType)){
             otf = new OracleTypeFactory();
            typeService = otf.produceProvider();
        }else if(DbEnum.mysql.toString().equals(dbType)){
             otf = new MysqlTypeFactory();
            typeService = otf.produceProvider();
        }else{
            System.out.println("暂不支持");
        }
        if(null != typeService){
            resultType = typeService.getType(type);
        }
        return  resultType;
    }
}
