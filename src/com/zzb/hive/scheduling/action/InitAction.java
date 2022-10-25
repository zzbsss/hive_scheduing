package com.zzb.hive.scheduling.action;

import com.zzb.hive.scheduling.enums.DbEnum;
import com.zzb.hive.scheduling.utils.DateUtil;
import com.zzb.hive.scheduling.vo.DataBaseInfo;
import com.zzb.hive.scheduling.vo.Task;
import java.io.*;
import java.util.*;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 * 类名:InitAction
 * 说明:加载配置并初始化任务
 *
 * @author zzb
 * @Date 2019年5月9日 17:49:09
 * 修改记录：初始化数据库信息、初始化任务返回值为List类型，增加mysql的数据库连接信息方法
 * @see
 **/
public class InitAction {
    //需要重试的任务
    public static List<Task> needRestartTask = new ArrayList<>();
    //是否进入重试模式
    public static boolean isRestart = false;
    //    获取数据库连接信息
    private static List<DataBaseInfo> initDb() {
        List<DataBaseInfo> list = new ArrayList<>();
        DataBaseInfo dbif;
        Map<String, String> hMap = new HashMap<>();
        Map<String, String> oMap = new HashMap<>();
        Map<String, String> mMap = new HashMap<>();
        List<Map> maps = new ArrayList<>();
        Properties prop = new Properties();

        InputStream in = null;

        try {
            in = new BufferedInputStream(new FileInputStream(new File(Hive2DbAction.configPath+"jdbc.properties")));
        } catch (FileNotFoundException e) {
          throw new RuntimeException("未找到配置文件"+ Hive2DbAction.configPath+"jdbc.properties");
        }
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Iterator<String> it = prop.stringPropertyNames().iterator();
        while (it.hasNext()) {
            String key = it.next();
            String value = prop.getProperty(key);
            if (key.contains(DbEnum.hive.toString())) {
                hMap.put(key.substring(key.indexOf(".") + 1), value);
            }
            if (key.startsWith(DbEnum.mysql.toString())) {
                mMap.put(key.substring(key.indexOf(".") + 1), value);
            }
            if (key.contains(DbEnum.oracle.toString())) {
                oMap.put(key.substring(key.indexOf(".") + 1), value);
            }
        }
        maps.add(hMap);
        maps.add(oMap);
        maps.add(mMap);
        for (int i = 0; i < 3; i++) {
            dbif = new DataBaseInfo();
            switch (i){
                case 0 : dbif.setDbType(DbEnum.hive); break;
                case 1 : dbif.setDbType(DbEnum.oracle); break;
                case 2 : dbif.setDbType(DbEnum.mysql); break;
            }
            dbif.setDriverClass(maps.get(i).get("driverClass").toString());
            dbif.setPassword(maps.get(i).get("password").toString());
            dbif.setUrl(maps.get(i).get("url").toString());
            dbif.setUserName(maps.get(i).get("userName").toString());
            list.add(dbif);
        }
        return list;
    }

    //    获取hive数据库连接信息
    public static DataBaseInfo initHive() {
        DataBaseInfo dbif = InitAction.initDb().get(0);
        return dbif;
    }

    //    获取oracle数据库连接信息
    public static DataBaseInfo initOracle() {
        DataBaseInfo dbif = InitAction.initDb().get(1);
        return dbif;
    }

    //    获取mysql数据库连接信息
    public static DataBaseInfo initMysql() {
        DataBaseInfo dbif = InitAction.initDb().get(2);
        return dbif;
    }

    //初始化任务
    public static List<Task> initTask(String taskConfig) {
        List<Task> list = new ArrayList<>();
        SAXReader reader = new SAXReader();
        InputStream in = null;
        try {
            in = new FileInputStream(new File(Hive2DbAction.configPath+taskConfig));
        } catch (FileNotFoundException e) {
            System.out.println("没有找到任务配置文件");
            e.printStackTrace();
        }
        Document document = null;
        try {
            document = reader.read(in);
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        Element root = document.getRootElement();
        List<Element> childElements = root.elements();
        for (Element child : childElements) {
            List<String> te = new ArrayList<>();
            List<Element> elementList = child.elements();
            for (Element ele : elementList) {
                te.add(ele.getText());
            }
            Task task = new Task();
            task.setId(te.get(0));
            task.setHive_id(te.get(1));
            task.setDbname(te.get(2));
            task.setScript(te.get(3));
            task.setIsIncre(te.get(4));
            task.setPreSql(te.get(5));
            task.setSchedule(te.get(6));
            task.setDesc(te.get(7));
            task.setRunTime(te.get(8));
            task.setReRunMaxTimes(Integer.parseInt(te.get(9)));
            task.setLogPath(te.get(10));
            task.setColumns(te.get(11));
            task.setRunHiveSuccess(false);
            task.setRunHiveTimes(0);
            task.setRunSqoopSuccess(false);
            list.add(task);
        }
        return getNeedTasks(list);
    }

    /**
     * 得到需要的task
     * @param list
     * @return
     * 今天不是月一号，排除按月执行的任务
     * 今天不是周几，就排除周几的任务
     */
    private static List<Task>  getNeedTasks(List<Task> list) {
        System.out.println("读取配置文件的集市名：");
        list.forEach(task ->{
            System.out.println(task.getId());
        });
        System.out.println(list.size());
        List<Task> tasks = removeMonTask(list);
        //System.out.println("移除月执行的数据之后"+tasks) ;
        List<Task> tasks1 = removeWeekTask(tasks);
        return removeQuarter(tasks1);
    }

    /**
     * 年的任意一天任务判断（也可指定月和日，中间用逗号隔开）
     * @param tasks1
     * @return
     */
    private static  List<Task> removeQuarter(List<Task> tasks1) {
        String currMonDay = DateUtil.getCurrMonDay(); //0527
        Iterator<Task> iterator = tasks1.iterator();
        while(iterator.hasNext()){
            Task task = iterator.next();
            String runTime = task.getRunTime().trim();
                if("年".equals(runTime.substring(0,1))){
                    if(!(runTime.substring(1).contains(currMonDay))){
                        iterator.remove();
                    }
                }
            }
    return tasks1;
    }

    /**
     * 排除按月（月1号就是01 26号就是26）执行的任务
     * @param list
     * @return
     */
    private static List<Task> removeMonTask(List<Task> list) {
        // boolean isFirstDayOfMon = DateUtil.isFirstDayOfMon();
        String currDay = DateUtil.getCurrDay();
        currDay = "月" + currDay;
        Iterator<Task> iterator = list.iterator();
        while (iterator.hasNext()){
            Task task = iterator.next();
            String runTime = task.getRunTime().trim();
            if("月".equals(runTime.substring(0,1))){
                if(!(runTime.equals(currDay))){
                    iterator.remove();
                }
            }
        }
        return list;
    }

    /**
     * 移除周几才运行的任务
     * @return
     */
    private static  List<Task> removeWeekTask(List<Task> list){
        String weekDay = DateUtil.getWeekDay();
        Iterator<Task> iterator = list.iterator();
        while (iterator.hasNext()){
            Task task = iterator.next();
            String runTime = task.getRunTime().trim();
            if("周".equals(runTime.substring(0,1))){
                if(!(runTime.equals(weekDay))){
                    iterator.remove();
                }
            }
        }
        return list;
    }
}
