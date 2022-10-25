package com.zzb.hive.scheduling.utils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzManager {
    private static SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    private static String JOB_GROUP_NAME = "RUN_SCRIPT"; //任务组名称
    private static String TRIGGER_GROUP_NAME = "MDRH"; //触发器组名

    /**
     * 添加一个定时任务，使用默认的任务组名，触发器组名
     * @param jobName
     * @param cls
     * @param time
     */
    public static void addJob(String jobName, Class<? extends Job> cls,String time){
        try {
            Scheduler scheduler = schedulerFactory.getScheduler();
            JobDetail jobDetail = JobBuilder.newJob(cls).withIdentity(jobName,JOB_GROUP_NAME).build();
            CronTrigger trigger = (CronTrigger) TriggerBuilder.
                    newTrigger().withIdentity(jobName,TRIGGER_GROUP_NAME)
                    .withSchedule(CronScheduleBuilder.cronSchedule(time))
                    .build();
            scheduler.scheduleJob(jobDetail,trigger);
            if(!scheduler.isShutdown()){
                scheduler.start();
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
    public static void addJob(String jobName, Class<? extends Job> cls, String time, Object parameter){
        try {
            Scheduler scheduler = schedulerFactory.getScheduler();
            JobDetail jobDetail = JobBuilder.newJob(cls).withIdentity(jobName, JOB_GROUP_NAME).build();
            jobDetail.getJobDataMap().put("parameter",parameter);
            CronTrigger trigger = (CronTrigger) TriggerBuilder
                    .newTrigger()
                    .withIdentity(jobName,JOB_GROUP_NAME)
                    .withSchedule(CronScheduleBuilder.cronSchedule(time))
                    .build();
            scheduler.scheduleJob(jobDetail,trigger);
            if(!scheduler.isShutdown()){
                scheduler.start();
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

    }
}
