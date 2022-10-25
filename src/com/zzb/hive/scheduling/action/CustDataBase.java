package com.zzb.hive.scheduling.action;

import com.zzb.hive.scheduling.vo.DataBaseInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 类名:CustDataBase
 * 说明:数据库连接类
 *
 * @author zzb
 * @Date 2019/5/14 9:39
 *    修改记录：
 *  *
 *  * @see
 **/
public class CustDataBase {

    //    数据库连接对象
    private static Connection conn;

    //    关闭数据库连接
    public static void  closeConn(Connection conn) {
        if (conn!=null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //    获取数据库连接
        public static Connection getConnection(DataBaseInfo dataBaseInfo) {
        try {
            Class.forName(dataBaseInfo.getDriverClass());
            conn = DriverManager.getConnection(dataBaseInfo.getUrl(), dataBaseInfo.getUserName(), dataBaseInfo.getPassword());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    //    temp overload
    public Connection getConnection(String classLoad, String path, String username, String password) {
        return null;
    }
}
