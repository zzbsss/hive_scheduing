package com.zzb.hive.scheduling.typeimpl;

import com.zzb.hive.scheduling.hander.TypeHander;

/**
 * varchar类型处理
 */
public class VarChar2Type implements TypeHander {
    @Override
    public String getTypeName(String type) {
        String result;
        int varcharCount = Integer.parseInt(type
                .replace("varchar", "").replace("(", "")
                .replace(")", ""));
        if ((type.equals("varchar(4000)")) ||
                (varcharCount > 4000))
            result = "varchar2(4000)";
        else
            result = type.replace("varchar", "nvarchar2");
        return result;
    }
}
