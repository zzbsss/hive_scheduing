package com.zzb.hive.scheduling.factory;

import com.zzb.hive.scheduling.provider.TypeProvider;
import com.zzb.hive.scheduling.service.TypeService;
import com.zzb.hive.scheduling.service.impl.MySqlTypeServiceImpl;

/**
 * mysql类型处理服务工厂
 */
public class MysqlTypeFactory implements TypeProvider {
    @Override
    public TypeService produceProvider() {
        return new MySqlTypeServiceImpl();
    }
}
