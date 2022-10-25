package com.zzb.hive.scheduling.service.impl;

import com.zzb.hive.scheduling.action.CustDataBase;
import com.zzb.hive.scheduling.action.Hive2DbAction;
import com.zzb.hive.scheduling.action.InitAction;
import com.zzb.hive.scheduling.service.ColumnTypeService;
import com.zzb.hive.scheduling.service.TableService;
import com.zzb.hive.scheduling.utils.UUIDutils;
import com.zzb.hive.scheduling.vo.Column;
import com.zzb.hive.scheduling.vo.Table;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableServiceImpl implements TableService {

    public String getScript(String path, Table hiveTable, Table dbTable, String dbType) {
        String tbName = hiveTable.getName();
        File logFile = Hive2DbAction.getLogFile(path);
        String script = null;
        if(null == dbTable){
            //生成建表的语句
            System.out.println("在oracle中该表不存在，正在生成建表语句.......");
            Hive2DbAction.logMsg(logFile,tbName+"该表不存在，生成建表语句");
             script = createTableScript(logFile, hiveTable,dbType);
        }else{
            //判断是否有新增的字段
            List<Column> hColumns = hiveTable.getColumns();
            ArrayList<String> hcNames = new ArrayList<>();
            List<Column> oColumns = dbTable.getColumns();
            ArrayList<String> ocNames = new ArrayList<>();
            hColumns.forEach((column) ->{
                hcNames.add(column.getName().toUpperCase());
            });

            oColumns.forEach((column) -> {
                ocNames.add(column.getName().toUpperCase());
            });
            if(hcNames.size() == ocNames.size()){
                //没有新增字段,执行一段临时脚本
                Hive2DbAction.logMsg(logFile,"没有新增字段");
                script = "select 1 from dual";
            }else{
                //取差集
                hcNames.removeAll(ocNames);
                if(hcNames.size() == 0) {Hive2DbAction.logMsg(logFile,"请检查oracle中字段"+ Arrays.asList(hcNames));}
                Hive2DbAction.logMsg(logFile,"新增的字段是"+ Arrays.asList(hcNames));
                script = addOrclColumn(tbName, logFile, hColumns, hcNames,dbType);
            }
        }
        return script;
    }

    private String addOrclColumn(String tbName,File  logFile, List<Column> hColumns, ArrayList<String> hcNames,String dbType) {
        ColumnTypeService cts = new ColumnTypeServiceImpl();
        String script = null;
        ArrayList<Column> needColumn = new ArrayList<>();
            StringBuffer sb = new StringBuffer();
            hColumns.forEach((column) -> {
                hcNames.forEach((name)->{
                    if(column.getName().equalsIgnoreCase(name))
                        needColumn.add(column);
                });
            });
            needColumn.forEach((column) -> {
                String column_name = column.getName();
                String type_name = cts.getColumnType(column, dbType);
                sb.append("alter table "+tbName+" add "+column_name+" "+type_name+";");
            });
            Hive2DbAction.logMsg(logFile,"新增行字段脚本为"+(script=sb.toString()));
            Hive2DbAction.logMsg(logFile,tbName+"生成新增字段的脚本完成");
        return script;
    }

    /**
     * 生成建表的语句
     */
    private String createTableScript(File logFile,Table hiveTable,String dbType) {
        ColumnTypeService cts = new ColumnTypeServiceImpl();
        String tbName = hiveTable.getName();
        String  sql = "create table " + tbName + "(";
        List<Column> columns = hiveTable.getColumns();
        StringBuilder sb  = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            String cName = column.getName();
            String desc = column.getDesc();
            boolean isPk = column.getIsPk();
            String type_name = cts.getColumnType(column,dbType);
            sb.append("COMMENT ON COLUMN "+tbName+"."+cName+" IS "+"'"+desc+"'"+";");
            sql += isPk?cName + " " + type_name + "primary key,":cName + " " + type_name;
            if(i != columns.size()-1){
                sql += ",";
            }
        }
        sql = sql+ ");";
        sql = sql + sb.toString();
        Hive2DbAction.logMsg(logFile,"建表的脚本为"+sql);
        return sql;
    }


    /**
     * Hive表结构
     * @param tbName
     * @param databaseName
     * @return
     */
    public Table getHTable(String tbName,String databaseName,String path) {
        Table table = new Table();
        ArrayList<Column> columns = new ArrayList<>();
        table.setCode(UUIDutils.getUUID());
        table.setName(tbName);
        table.setDesc(tbName);
        table.setId(UUIDutils.getUUID());
        File logFile = Hive2DbAction.getLogFile(path);
        PreparedStatement ps = null;
        //Connection hiveCon = CustDataBase.getConnection(Hive2DbAction.classLoad, Hive2DbAction.path, Hive2DbAction.username, Hive2DbAction.password);
        Connection hiveCon = CustDataBase.getConnection(InitAction.initHive());
        String sqlField = "select c.column_name,c.type_name,c.comment from COLUMNS_V2 c join SDS s on c.CD_ID=s.CD_ID join TBLS t on t.SD_ID=s.SD_ID join DBS d on d.DB_ID=t.DB_ID where 1=1 and d.name='" +
                databaseName +
                "' and t.TBL_NAME='" +
                tbName +
                "' order by c.INTEGER_IDX asc";
        try {
            ps = hiveCon.prepareStatement(sqlField);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Column column = new Column();
                String column_name = rs.getString("column_name");
                String type_name = rs.getString("type_name");
                String comment = rs.getString("comment");
                column.setType(type_name);
                column.setCode(type_name);
                column.setDesc(comment);
                column.setId(UUIDutils.getUUID());
                column.setIsPk("PK_ID".equals(column_name.toUpperCase()));
                column.setName(column_name);
                columns.add(column);
            }
            table.setColumns(columns);
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //Hive2DbAction.logMsg(logFile,tbName+"的hive的表结构为"+table);
        Hive2DbAction.logMsg(logFile,tbName+"成功拿到hive表结构");
        CustDataBase.closeConn(hiveCon);
        return table;
    }

    /**
     * oracle 表结构
     * @param tbName
     * @return
     */
    @Override
    public Table getOTable(String tbName,String path) {
        File logFile = Hive2DbAction.getLogFile(path);
        //Connection orclConn = CustDataBase.getConnection(Hive2DbAction.oracleClassLoad, Hive2DbAction.oraclePath, Hive2DbAction.oracleUsername, Hive2DbAction.oraclePassword);
        Connection orclConn = CustDataBase.getConnection(InitAction.initOracle());
        boolean flag = isExistsTableOfOracle(tbName);
        PreparedStatement ps = null;
        if(!flag){
            Hive2DbAction.logMsg(logFile,"oracle不存在该表"+tbName);
            return null;
        }else{
            Table table = new Table();
            ArrayList<Column> columns = new ArrayList<>();
            String sql = "select column_name,data_type from user_tab_cols where table_name='"+tbName.toUpperCase()+"'";
            try {
                ps = orclConn.prepareStatement(sql);
                ResultSet rs = ps.executeQuery();
                while (rs.next()){
                    Column column = new Column();
                    String column_name = rs.getString("column_name");
                    String type_name = rs.getString("data_type");
                    column.setType(type_name);
                    column.setName(column_name);
                    column.setId(UUIDutils.getUUID());
                    column.setDesc(column_name);
                    column.setCode(type_name);
                    column.setIsPk("PK_ID".equals(column_name.toUpperCase()));
                    columns.add(column);
                }
                table.setCode(tbName);
                table.setName(tbName);
                table.setId(UUIDutils.getUUID());
                table.setDesc(tbName);
                table.setColumns(columns);
            } catch (SQLException e) {
                e.printStackTrace();
            }
           // Hive2DbAction.logMsg(logFile,tbName+"的Oracle的表结构为"+table);
            Hive2DbAction.logMsg(logFile,tbName+"成功拿到oracle表结构");
            CustDataBase.closeConn(orclConn);
            return table;
        }
    }

    /**
     * oracle 表是否存在
     * @param tableName
     * @return
     */
    public static boolean isExistsTableOfOracle(String tableName) {
        //Connection orclConn = CustDataBase.getConnection(Hive2DbAction.oracleClassLoad, Hive2DbAction.oraclePath, Hive2DbAction.oracleUsername, Hive2DbAction.oraclePassword);
        Connection orclConn = CustDataBase.getConnection(InitAction.initOracle());
        PreparedStatement ps = null;
        boolean isExists = false;
        String sql = "select count(*) from user_tables where table_name='" +
                tableName.toUpperCase() + "'";
        try {
            ps = orclConn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            int count = 0;
            if (rs.next()) {
                count = rs.getInt(1);
            }
            if (count > 0)
                isExists = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        CustDataBase.closeConn(orclConn);
        return isExists;
    }

}
