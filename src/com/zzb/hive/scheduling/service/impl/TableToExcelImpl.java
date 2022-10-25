package com.zzb.hive.scheduling.service.impl;

import com.zzb.hive.scheduling.action.CustDataBase;
import com.zzb.hive.scheduling.action.InitAction;
import com.zzb.hive.scheduling.enums.DbEnum;
import com.zzb.hive.scheduling.service.TableToExcelService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 类名:TableToExcelImpl
 * 说明:
 *
 * @author zzb
 * @Date 2019/5/17 18:30
 * 修改记录：
 * @see
 **/
public class TableToExcelImpl implements TableToExcelService {


    @Override
    public List<String[]> queryTableStructure(String tbName, String... args) {
        List<String[]> list = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String sql = null;
        Connection conn = null;
        if (args[0].equals(DbEnum.oracle.toString())) {
            if (TableServiceImpl.isExistsTableOfOracle(tbName)){
            sql = "select q1.COLUMN_NAME, q1.DATA_TYPE, q1.NULLABLE, q1.COMMENTS, q2.CONSTRAINT_NAME " +
                    "from (select t1.table_name table_name, " +
                    "t1.COLUMN_NAME COLUMN_NAME, " +
                    "t1.DATA_TYPE DATA_TYPE, " +
                    "t1.NULLABLE NULLABLE, " +
                    "ta.COMMENTS COMMENTS " +
                    "from user_tab_columns t1, user_col_comments ta " +
                    "where t1.table_name = ta.table_name and t1.column_name = ta.column_name) q1 " +
                    "left join (select t2.table_name table_name, " +
                    "tb.column_name column_name, " +
                    "tb.CONSTRAINT_NAME CONSTRAINT_NAME " +
                    "from user_constraints t2, user_cons_columns tb " +
                    "where t2.constraint_name = tb.constraint_name and t2.constraint_type = 'P') q2 " +
                    "on q1.TABLE_NAME=q2.table_name and q1.column_name = q2.column_name " +
                    "where q1.table_name='"+tbName+"'";
            conn = CustDataBase.getConnection(InitAction.initOracle());
            }else{
                throw new RuntimeException("Oracle表不存在："+tbName);
            }
        } else if (args[0].equals(DbEnum.hive.toString())) {
            sql = "select c.column_name,c.type_name,c.comment from COLUMNS_V2 c join SDS s on c.CD_ID=s.CD_ID join TBLS t on t.SD_ID=s.SD_ID join DBS d on d.DB_ID=t.DB_ID where 1=1 and d.name='" +
                    args[1] +
                    "' and t.TBL_NAME='" +
                    tbName +
                    "' order by c.INTEGER_IDX asc";
            conn = CustDataBase.getConnection(InitAction.initHive());
        }else{
            throw new RuntimeException("没有获取到数据库连接！");
        }
        try {
            preparedStatement = conn.prepareStatement(sql);
            // System.out.println(sql);
            //preparedStatement.setString(1, tbName);
            resultSet = preparedStatement.executeQuery();
            String[] tabStructure;
            while (resultSet.next()) {
                tabStructure = new String[5];
                if (args[0].equals(DbEnum.oracle.toString())) {
                    tabStructure[0] = resultSet.getString(1);
                    tabStructure[1] = resultSet.getString(2);
                    tabStructure[2] = resultSet.getString(3);
                    tabStructure[3] = resultSet.getString(4);
                    tabStructure[4] = resultSet.getString(5);
                } else if (args[0].equals(DbEnum.hive.toString())) {
                    tabStructure[0] = resultSet.getString(1);
                    tabStructure[1] = resultSet.getString(2);
                    tabStructure[3] = resultSet.getString(3);
                    tabStructure[2] = tabStructure[4] = "";
                }
                System.out.println();
                list.add(tabStructure);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            CustDataBase.closeConn(conn);
        }
        System.out.println("获取表结构信息完成！");
        return list;
    }

    @Override
    public String getType(String type) {
        return null;
    }
}
