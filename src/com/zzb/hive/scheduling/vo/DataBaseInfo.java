package com.zzb.hive.scheduling.vo;

import com.zzb.hive.scheduling.enums.DbEnum;

/**
 * 类名:DataBaseInfo
 * 说明:数据库连接信息实体类
 *
 * @author zzb
 * @Date 2019年5月9日 17:49:09
 *    修改记录：
 *  *
 *  * @see
 **/
public class DataBaseInfo {

    //    数据库类型
    private DbEnum dbType;

    //    数据库驱动
    private String driverClass;

    //    数据库密码
    private String password;

    //    数据库连接地址
    private String url;

    //    数据库名称
    private String userName;

    public DbEnum getDbType() {
        return dbType;
    }

    public void setDbType(DbEnum dbType) {
        this.dbType = dbType;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return "DataBaseInfo{" +
                "driverClass='" + driverClass + '\'' +
                ", password='" + password + '\'' +
                ", url='" + url + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }
}
