package com.zzb.hive.scheduling.service;

import java.util.List;

/**
 * 类名:TableToExcelService
 * 说明:
 *
 * @author ll
 * @Date 2019/5/17 18:30
 * 修改记录：
 * @see
 **/
public interface TableToExcelService extends TypeService {

    /**
     * <p>说明:   查询数据库表结构信息</p><br/>
     * <p>参数:   [tbName, args]表名(是必需的)，可选参数[数据库类型(是必需的)][数据库名称(若类型为hive则是必需的)]</p><br/>
     * <p>返回值: java.util.List<java.lang.String[]>返回数组的集合</p><br/>
     * <p>改进:   </p>
     */
    List<String[]> queryTableStructure(String tbName, String... args);


}
