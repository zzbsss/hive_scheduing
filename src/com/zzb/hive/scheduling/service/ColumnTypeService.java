package com.zzb.hive.scheduling.service;

import com.zzb.hive.scheduling.vo.Column;

public interface ColumnTypeService {

    String getColumnType(Column column, String dbType);
}
