package com.zzb.hive.scheduling.service;

import com.zzb.hive.scheduling.vo.Table;

public interface TableService {

    String getScript(String path, Table hiveTable, Table dbTable, String dbType);
    //获取hive表信息
    Table getHTable(String tableName, String databaseName,String path);
    //获取oracle表信息
    Table getOTable(String tbName,String path);
}
