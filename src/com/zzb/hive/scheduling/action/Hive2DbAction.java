package com.zzb.hive.scheduling.action;
import com.zzb.hive.scheduling.service.ScriptService;
import com.zzb.hive.scheduling.service.impl.ScriptServiceImpl;
import com.zzb.hive.scheduling.vo.DataBaseInfo;
import com.zzb.hive.scheduling.vo.Task;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName Hive2DbAction
 * @Description TODO
 * @Auther zzb
 * @Date 2019/5/13 13:12
 * @Version 1.0
 **/
public class Hive2DbAction {
    public static String classLoad = "";
    public static String path = "";

    public static String username = "";
    public static String password = "";

    public static boolean initHive = false;

    public static String oracleClassLoad = "";
    public static String oraclePath = "";
    public static String oracleUsername = "";
    public static String oraclePassword = "";
    public static String dirPath = "";
    public static String sqoopLogsPath = "";
    public static String configPath = "";
    //是否只要执行hive语句不执行推送语句
    public static boolean onlyRunHive = false;

    public static SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) {
//        String rePath = Hive2DbAction.class.getClassLoader().getResource("jdbc.properties").getPath();
        setVarByProperties();
        String taskConfig = args[0];
        onlyRunHive = Boolean.valueOf(args[1]);
        ScriptService  ss  = new ScriptServiceImpl();
        List<Task> tasks = InitAction.initTask(taskConfig);
        System.out.println("当前要跑的脚本集市名:");
        tasks.forEach(task ->{
            System.out.println(task.getId());
        });
        System.out.println(tasks.size());
        ss.run(tasks);
        //开始运行失败的任务
        List<Task> needRestartTasks = InitAction.needRestartTask;
        if(needRestartTasks.size()>0){
            InitAction.isRestart = true;
            System.out.println("重试"+needRestartTasks.size()+"个任务开始运行");
            ss.run(needRestartTasks);
        }
      /*  needRestartTasks.forEach((task) -> {
            QuartzManager.addJob(task.getId(),RunJob.class,task.getSchedule(),task);

        });*/
    }


    /**
     * 初始化参数
     *
     * @param
     */
    private static void setVarByProperties() {
            /*FileInputStream inputStream = new FileInputStream(jdbcFilePath);
            Properties properties = new Properties();
            properties.load(inputStream);*/
        Properties prop = new Properties();
        InputStream in = new BufferedInputStream(InitAction.class.getClassLoader().getResourceAsStream("runpath.properties"));
        try {
            prop.load(in);
            Hive2DbAction.dirPath = prop.getProperty("runpath");
            Hive2DbAction.sqoopLogsPath = prop.getProperty("sqoop_logs_path");
            Hive2DbAction.configPath = prop.getProperty("config_path");
            Hive2DbAction.initHive = Boolean.valueOf(prop.getProperty("init_hive"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        DataBaseInfo hDbif = InitAction.initHive();
        DataBaseInfo oDbif = InitAction.initOracle();
        Hive2DbAction.classLoad = hDbif.getDriverClass();
        Hive2DbAction.path = hDbif.getUrl();
        Hive2DbAction.username = hDbif.getUserName();
        Hive2DbAction.password = hDbif.getPassword();
        Hive2DbAction.oracleClassLoad = oDbif.getDriverClass();
        Hive2DbAction.oraclePath = oDbif.getUrl();
        Hive2DbAction.oracleUsername = oDbif.getUserName();
        Hive2DbAction.oraclePassword = oDbif.getPassword();
    }

    public static File getLogFile(String name) {
        String path = dirPath + "/" + sf.format(new Date());
        File logFile = new File(path + "/" + name + ".log");
        File fileParent = new File(path);
        try {
            if (!fileParent.exists()) {
                fileParent.mkdirs();
            }
            if (!logFile.exists())
                logFile.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return logFile;
    }

    public static void logMsg(File logFile, String str) {
        if (logFile == null)
            throw new IllegalStateException("logFile can not be null!");
        try {
            Writer txWriter = new FileWriter(logFile, true);
            txWriter.write(str + "\r\n");
            txWriter.flush();
            txWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
