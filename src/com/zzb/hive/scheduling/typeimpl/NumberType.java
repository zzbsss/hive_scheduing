package com.zzb.hive.scheduling.typeimpl;

import com.zzb.hive.scheduling.hander.TypeHander;

/**
 * number类型处理
 */
public class NumberType implements TypeHander {

    @Override
    public String getTypeName(String type) {
        String result;
       if (type.contains("decimal")) {
           result = type.replace("decimal", "number");
        } else if (type.contains("double")) {
           result = type.replace("double", "number(38,9)");
        } else if (type.contains("bigint")) {
           result = type.replace("bigint", "number(38,0)");
        }else if(type.contains("date")) {
           result = "date";
        }else if(type.contains("int")){
           result = "integer";
        }else{
           result = "varchar2(4000)";
        }
        return result;
    }
}
