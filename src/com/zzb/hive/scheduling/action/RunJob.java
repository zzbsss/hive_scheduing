package com.zzb.hive.scheduling.action;

import com.zzb.hive.scheduling.service.ScriptService;
import com.zzb.hive.scheduling.service.impl.ScriptServiceImpl;
import com.zzb.hive.scheduling.vo.Task;
import java.text.SimpleDateFormat;

import org.quartz.*;

public class RunJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ScriptService ss = new ScriptServiceImpl();
        JobDetail jobDetail = jobExecutionContext.getJobDetail();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        Task task = (Task) jobDataMap.get("parameter");
        Trigger currTrigger = jobExecutionContext.getTrigger();
        String tbName = task.getId();
        Hive2DbAction.logMsg(Hive2DbAction.getLogFile(tbName),sdf.format(currTrigger.getStartTime())+tbName+"表数据重试推送开始运行");
        ss.run(task);
        Hive2DbAction.logMsg(Hive2DbAction.getLogFile(tbName),"===============================================================================================");
    }
}
