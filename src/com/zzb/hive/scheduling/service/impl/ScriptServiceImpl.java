package com.zzb.hive.scheduling.service.impl;

import com.zzb.hive.scheduling.action.CustDataBase;
import com.zzb.hive.scheduling.action.ExportAction;
import com.zzb.hive.scheduling.action.Hive2DbAction;
import com.zzb.hive.scheduling.action.InitAction;
import com.zzb.hive.scheduling.enums.DbEnum;
import com.zzb.hive.scheduling.service.ScriptService;
import com.zzb.hive.scheduling.service.TableService;
import com.zzb.hive.scheduling.vo.Column;
import com.zzb.hive.scheduling.vo.DataBaseInfo;
import com.zzb.hive.scheduling.vo.Table;
import com.zzb.hive.scheduling.vo.Task;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ScriptServiceImpl  implements ScriptService {
    private TableService ts = new TableServiceImpl();
    private GlobalLogImpl gl =  new GlobalLogImpl();
    //private TableToExcelImpl tte = new TableToExcelImpl();

    /**
     * 一次性全部运行,并导出表结构到Excel
     * @param tasks
     */
    public void run(List<Task> tasks) {
        double size = 0.0;
        if(null != tasks){
            size = tasks.size();
        }
        BigDecimal decimalSize = new BigDecimal(size);
        int count = 0;
        SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.err.println(getProgressBar(0,sdf));
        for (Task task : tasks) {
            count++;
            String tbName = task.getId();
            String logPath = task.getDesc().substring(0,task.getDesc().lastIndexOf("."));
            if(InitAction.isRestart){
                int reRunMaxTimes = task.getReRunMaxTimes();
                for(int i =0; i< reRunMaxTimes;i++){
                    boolean runHiveSuccess = task.getRunHiveSuccess();
                    if(runHiveSuccess){
                        break;
                    }else{
                        Hive2DbAction.logMsg(Hive2DbAction.getLogFile(logPath),sdf.format(new Date())+tbName+"重试开始运行");
                        //exportToExcel(task, tbName);
                        Hive2DbAction.logMsg(Hive2DbAction.getLogFile(logPath),"===============================================================================================");
                        runScript(task);
                    }
                }
            }else{
                Hive2DbAction.logMsg(Hive2DbAction.getLogFile(logPath),sdf.format(new Date())+tbName+"表数据推送开始运行");
                runScript(task);
                //exportToExcel(task, tbName);
                Hive2DbAction.logMsg(Hive2DbAction.getLogFile(logPath),"===============================================================================================");
                System.err.println(getProgressBar(new BigDecimal(count).divide(decimalSize,3,BigDecimal.ROUND_HALF_UP).doubleValue(),sdf));
            }
        }

    }

    private void exportToExcel(Task task, String tbName) {
        ExportAction exportAction =  new ExportAction();
        exportAction.exportToExcel(tbName, DbEnum.hive.toString(), task.getDbname());
        exportAction.exportToExcel(tbName, DbEnum.oracle.toString());
    }


    /**
     * 定时运行
     * @param task
     */
    @Override
    public void run(Task task) {
       runScript(task);
    }

    private String getProgressBar(double val,SimpleDateFormat sdf) {
        String result = null;
        String currVal = val*100+"%";
        String str = null;
        if(val == 0){
            str = "程序开始时间 ："+sdf.format(new Date())+"\n"+"==>>==================";
        }else if(val<=0.1){
            str =  "==>>==================";
        }else if(val <= 0.2){
            str =  "====>>================";
        }else if(val <= 0.3){
            str = "======>>==============";
        }else if(val <= 0.4){
            str =  "========>>============";
        }else if(val <= 0.5){
            str = "==========>>==========";
        }else if(val <= 0.6){
            str = "============>>========";
        }else if(val <= 0.7){
            str = "==============>>======";
        }else if(val <= 0.8){
            str = "================>>====";
        }else if(val <= 0.9){
            str =  "===================>>==";
        }else{
            str = "程序结束时间："+sdf.format(new Date())+"\n"+"=====================";
        }
        result = str+currVal;
        return result;
    }

    /**
     * 跑脚本的核心方法
     * @param task
     */
    private void runScript(Task task){
        String tbName = task.getId();
        String path = task.getDesc().substring(0,task.getDesc().lastIndexOf("."));
        File logFile = Hive2DbAction.getLogFile(path);
        String hScriptPath = task.getScript();
        StringBuilder sb = new StringBuilder();
        boolean isRestart = InitAction.isRestart;
        String logPath = Hive2DbAction.dirPath + task.getLogPath()+"/"+Hive2DbAction.sf.format(new Date())+ "/" + path+".log";
        delLogFile(isRestart, logPath);
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(hScriptPath)));
            String line;
            String nextLine = "sleep 8";
            boolean flag = false;
            boolean haveSqlLine = false;
            while (null!=(line = br.readLine())){
                sb.append(line+"\n");
                if(flag){
                    nextLine = line;
                    break;
                }
                if(haveSqlLine=(line.contains(path))) {
                    Hive2DbAction.logMsg(logFile, "准备执行" + tbName + "的hive脚本");
                    String[] script = {"sh", "-c", line};
                    Process runScript = Runtime.getRuntime().exec(script);
                    runScript.waitFor();
                    runScript.destroy();
                    flag = true;
                }
            }
            //是否有运行脚本语句
            if(!haveSqlLine){
                try {
                    throw new RuntimeException("没有对应推送的语句请检查"+task.getScript()+"文件");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }else{
                String[] script = {"sh", "-c", nextLine};
                Process process = Runtime.getRuntime().exec(script);
                process.waitFor();
                process.destroy();
                boolean isSuccess = readTaskHiveLog(logPath);
                if(isRestart){
                    task.setRunHiveTimes(task.getRunHiveTimes()+1);
                    if(!isSuccess){
                        task.setRunHiveSuccess(false);
                        int runHiveTimes = task.getRunHiveTimes();
                        Hive2DbAction.logMsg(logFile,tbName+"hive执行失败"+task.getRunHiveTimes()+"次，请查看日志");
                        if(runHiveTimes == task.getReRunMaxTimes())
                            gl.writeGlobalLog(path+"hive脚本今天均执行失败了。。。。请注意",true);
                    }else{
                        if(!Hive2DbAction.onlyRunHive){
                            prepareRunSqoop(task, tbName, logFile);
                        }else{
                            String msg = absolutePath(logFile)+" 重试执行成功，程序只运行了hive";
                            Hive2DbAction.logMsg(logFile,msg);
                            gl.writeGlobalLog(msg,false);
                        }
                    }
                }else{
                    task.setRunHiveTimes(1);
                    if(!isSuccess){
                        InitAction.needRestartTask.add(task);
                        Hive2DbAction.logMsg(logFile, absolutePath(logFile) +"hive执行失败，已添加至稍后重试的任务中");
                    }else {
                        if(!Hive2DbAction.onlyRunHive){
                            prepareRunSqoop(task, tbName, logFile);
                        }else{
                            String msg = absolutePath(logFile)+"执行成功，程序只运行了hive";
                            Hive2DbAction.logMsg(logFile,msg);
                            gl.writeGlobalLog(msg,false);
                            System.out.println(msg);
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            String msg = "未找到"+hScriptPath+"该脚本文件";
            Hive2DbAction.logMsg(logFile,msg);
            gl.writeGlobalLog(msg,true);
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String absolutePath(File logFile) {
        return logFile.getAbsolutePath();
    }


    private void prepareRunSqoop(Task task, String tbName, File logFile) {
        task.setRunHiveSuccess(true);
        Hive2DbAction.logMsg(logFile, tbName + "表脚本执行完成");
        System.out.println(tbName + " 脚本运行完成");
        if(Hive2DbAction.initHive){
            sqoopHiveToDbWithCheckTable(task);
        }else{
            sqoopHiveToDb(task);
        }

    }

    /**
     * 推送
     * @param task
     */
    private void sqoopHiveToDb(Task task) {
        //建表处理
        String tableName = task.getId();
        String databaseName = task.getDbname();
        DataBaseInfo dataBaseInfo = InitAction.initOracle();
        String isIncre = task.getIsIncre();
        Connection orclConn = CustDataBase.getConnection(dataBaseInfo);
        PreparedStatement ps= null;
        try {
            //推送数据预处理sql 1 全量 3 指定指定sql 2 增量
            String sql = "1".equals(isIncre)?"truncate table "+tableName:"3".equals(isIncre)?task.getPreSql():"select 1+1 from dual";
            System.out.println("这是预处理的sql: "+sql);
            ps = orclConn.prepareStatement(sql);
            ps.execute();
            execSqoop(task,null,databaseName);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            try {
                if(null != ps)
                    ps.close();
                CustDataBase.closeConn(orclConn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void execSqoop(Task task, Table hTable, String databaseName) {
        String tableName = task.getId();
        String hiveTbName = task.getHive_id();
        String path = task.getDesc().substring(0,task.getDesc().lastIndexOf("."));
        File logFile = Hive2DbAction.getLogFile(path);
        String sqoopLogDirFilePath = Hive2DbAction.sqoopLogsPath+"/"+Hive2DbAction.sf.format(new Date());
        createLogFile(sqoopLogDirFilePath,path);
        String sqoopLogFilePath = sqoopLogDirFilePath+"/"+path+".log";
        String column = "";
        if(Hive2DbAction.initHive){
            StringBuilder cols = new StringBuilder();
            List<Column> list = hTable.getColumns();
            int size = list.size();
            for (int i = 0; i < size; i++) {
                String col = list.get(i).getName().toUpperCase();
                cols.append((i != size-1)?col+",":col);
            }
            column = cols.toString();
        }else{
            column = task.getColumns();
        }
        toOracle(task, databaseName, tableName, hiveTbName, path, logFile, sqoopLogFilePath, column);
    }


    /**
     * 数据推送到oracle
     * @param task 任务详情
     * @param databaseName 数据库名
     * @param tableName 推送表名
     * @param hiveTbName
     * @param path
     * @param logFile
     * @param sqoopLogFilePath
     * @param columns
     */
    private void toOracle(Task task, String databaseName, String tableName, String hiveTbName, String path, File logFile, String sqoopLogFilePath, String columns) {
        String sqoop = "sqoop export --connect " + Hive2DbAction.oraclePath +
                " --username " + Hive2DbAction.oracleUsername + " --password " +
                Hive2DbAction.oraclePassword.replace("+", "\\+").replace("!", "\\!") +
                " --m 32 --table " + tableName.toUpperCase() +
                " --hcatalog-database " + databaseName +
                " --hcatalog-table " + hiveTbName.toLowerCase() +" --columns \""+columns.toUpperCase()+"\""+ ">" + sqoopLogFilePath + " 2>&1";
        Hive2DbAction.logMsg(logFile, "执行的sqoop语句如下: \n" + sqoop);
        String[] cmdSqoop = {"sh", "-c", sqoop};
        int reRunMaxTimes = task.getReRunMaxTimes();
        for (int i = 0; i < reRunMaxTimes; i++) {
            boolean runSqoopSuccess = task.getRunSqoopSuccess();
            if (!runSqoopSuccess) {
                Hive2DbAction.logMsg(logFile, "执行sqoop语句次数: " + (i + 1));
                if (i >= 1)
                    delLogFile(sqoopLogFilePath);
                exeScript(cmdSqoop);
                boolean isSuccess = readTaskSqoopLog(sqoopLogFilePath);
                task.setRunSqoopSuccess(isSuccess);
                if ((i + 1) == task.getReRunMaxTimes())
                    gl.writeGlobalLog(path + "执行scoop今天均执行失败了。。。。请注意", true);
            } else {
                Hive2DbAction.logMsg(logFile, tableName + "的数据导入完成");
                System.out.println(tableName + "数据导入完成.");
                gl.writeGlobalLog(path + "今天执行成功", false);
                break;
            }
        }
    }


    /**
     * 查找集市的行数
     * @param tbName
     * @return
     */
    private boolean checkTable(String tbName) {
        String sql = "select count(*) from mdrh_app."+tbName;
        Connection conn = CustDataBase.getConnection(InitAction.initMysql());
        PreparedStatement   ps = null;
        int result = 0;
        try {
             ps = conn.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            result = resultSet.getInt(1);
            System.out.println("执行完后的结果行数=="+result);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return (result>0);
    }

    /**
     * 重试hive脚本删除失败日志文件
     * @param isRestart
     * @param logPath
     */
    private void delLogFile(boolean isRestart, String logPath) {
        if(isRestart){
            File file = new File(logPath);
            file.delete();
        }
    }

    /**
     * 删除文件
     * @param logPath
     */
    private void delLogFile(String logPath) {
            File file = new File(logPath);
            file.delete();
    }
    /**
     * 通过日志文件去判断HIVE是否运行成功
     * @param logName
     */
    private boolean readTaskHiveLog(String logName) {
        File file = new File(logName);
        boolean flag = true;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            while (null != (line=br.readLine())){
                String lineUp = line.toUpperCase();
                if(lineUp.contains("FAILED")||lineUp.contains("CAUSED")||lineUp.contains("failed")){
                    flag = false;
                    break;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }
    private boolean readTaskSqoopLog(String logName) {
        File file = new File(logName);
        boolean flag = false;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = null;
            while (null != (line=br.readLine())){
                 if(line.contains("kafka.KafkaNotification:")){
                     flag = true;
                     break;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     *
     * @param task
     * 推送数据思路
     * 先判断在oracle中是否有这张表，
     *      如果有就判断字段是否增加，（拿到hive数据库，和oracle结构进行处理）
     *          字段增加，执行增加字段的方法
     *          字段为增加不做处理
     *      没有产生建表的语句
     * 运行sqoop语句
     *      判断是否增量
     *          否 清空表在运行
     *          是 直接运行
     *
     *
     */
    private void sqoopHiveToDbWithCheckTable(Task task) {
        boolean result = checkTable(task.getId());
        if(!result)
            return;
        //建表处理
        String tableName = task.getId();
        String path = task.getDesc().substring(0,task.getDesc().lastIndexOf("."));
        String databaseName = task.getDbname();
        Table hTable = ts.getHTable(tableName, databaseName,path);
        Table oTable = ts.getOTable(tableName,path);
        DataBaseInfo dataBaseInfo = InitAction.initOracle();
        String script = ts.getScript(path,hTable, oTable,dataBaseInfo.getDbType().toString());
        String isIncre = task.getIsIncre();
        //执行hive脚本是否成功
        boolean runScriptSuccess = ((null != hTable)&&(hTable.getColumns().size()!=0));
        //System.out.println(runScriptSuccess);
        if(!runScriptSuccess){
            try {
                throw new RuntimeException("hive脚本执行有误，请检查脚本"+task.getScript());
            }catch (Exception e){
                e.printStackTrace();
            }
        }else{
            //运行该脚本
          Connection orclConn = CustDataBase.getConnection(dataBaseInfo);
            PreparedStatement ps= null;
            try {
                //表空 执行建表，表没有空 是否有增加字段 有运行，没有查询
                if(null == oTable){
                    String [] sqls = script.split(";");
                    for (int i = 0; i < sqls.length; i++) {
                        ps = orclConn.prepareStatement(sqls[i]);
                        ps.execute();
                    }

                }else if(hTable.getColumns().size()==oTable.getColumns().size()){
                    ps = orclConn.prepareStatement(script);
                    ps.executeQuery();
                }else{
                    String[]  scripts = script.split(";");
                    for (String sql : scripts) {
                        ps = orclConn.prepareStatement(sql);
                        ps.execute();
                    }
                }
                //推送数据预处理sql 1 全量 3 指定指定sql 2 增量
                String sql = "1".equals(isIncre)?"truncate table "+tableName:"3".equals(isIncre)?task.getPreSql():"select 1+1 from dual";
                System.out.println("这是预处理的sql"+sql);
                ps = orclConn.prepareStatement(sql);
                ps.execute();
                execSqoop(task, hTable, databaseName);
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                try {
                    if(null != ps)
                        ps.close();
                    CustDataBase.closeConn(orclConn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建日志文件目录
     * @param sqoopLogFilePath
     */
    private void createLogFile(String sqoopLogFilePath,String tableName) {
        File file = new File(sqoopLogFilePath);
        File logFile = new File(sqoopLogFilePath +"/"+tableName+".log");
        if (!file.exists()) {
            file.mkdirs();
        }
        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * exeScript
     * @param cmdSqoop
     */
    private void exeScript(String[] cmdSqoop) {
        Process p = null;
        try {
            p = Runtime.getRuntime().exec(cmdSqoop);
            p.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(null != p)
                p.destroy();
        }
    }
}
