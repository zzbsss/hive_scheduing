package com.zzb.hive.scheduling.vo;

import java.io.Serializable;


/**
   * 类名:Task
   * 说明:定时任务信息
 *
 * @author zzb
 * @Date 2019年5月9日 下午6:49:09
    *    修改记录：增加表所在数据库名称字段
 *
 * @see
**/
public class Task implements Serializable {
	
	/** Hive表集市(默认)**/
	private String id;

	/** Hive表集市(更新后)**/
	private String hive_id;

	/** Hive数据库名称 **/
	private String dbname;
	
	/** HQL脚本路径 **/
	private String script;
	
	/** 增量/全量/运行指定sql **/
	private String isIncre;

	public String getPreSql() {
		return preSql;
	}

	public void setPreSql(String preSql) {
		this.preSql = preSql;
	}

	/**指定sql**/
	private String preSql;
	/** 定时任务表达式 **/
	private String schedule;
	/** 任务描述**/
	private String desc;
	/**按天执行，按月执行，按周几执行**/
	private String runTime;
	/**hive脚本运行次数**/
	private int runHiveTimes;
	/**hive运行日志路径**/
	private String logPath;
	/**运行hive脚本是否成功**/
	private boolean runHiveSuccess;
	/**重试hive脚本或推送最大次数 */
	private int reRunMaxTimes;
	/**运行sqoop是否成功*/
	private boolean runSqoopSuccess;
	/**列**/
	private String columns;

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getHive_id() {
		return hive_id;
	}

	public void setHive_id(String hive_id) {
		this.hive_id = hive_id;
	}

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getIsIncre() {
		return isIncre;
	}

	public void setIsIncre(String isIncre) {
		this.isIncre = isIncre;
	}

	public String getSchedule() {
		return schedule;
	}

	public void setSchedule(String schedule) {
		this.schedule = schedule;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getRunTime() {
		return runTime;
	}

	public void setRunTime(String runTime) {
		this.runTime = runTime;
	}

	public int getRunHiveTimes() {
		return runHiveTimes;
	}

	public void setRunHiveTimes(int runHiveTimes) {
		this.runHiveTimes = runHiveTimes;
	}

	public String getLogPath() {
		return logPath;
	}

	public void setLogPath(String logPath) {
		this.logPath = logPath;
	}

	public boolean getRunHiveSuccess() {
		return runHiveSuccess;
	}

	public void setRunHiveSuccess(boolean runHiveSuccess) {
		this.runHiveSuccess = runHiveSuccess;
	}

	public int getReRunMaxTimes() {
		return reRunMaxTimes;
	}

	public void setReRunMaxTimes(int reRunMaxTimes) {
		this.reRunMaxTimes = reRunMaxTimes;
	}

	public boolean getRunSqoopSuccess() {
		return runSqoopSuccess;
	}

	public void setRunSqoopSuccess(boolean runSqoopSuccess) {
		this.runSqoopSuccess = runSqoopSuccess;
	}



	public Task() {
	}


	public Task(String id, String hive_id, String dbname, String script, String isIncre, String preSql, String schedule, String desc, String runTime, int runHiveTimes, String logPath, boolean runHiveSuccess, int reRunMaxTimes, boolean runSqoopSuccess) {
		this.id = id;
		this.hive_id = hive_id;
		this.dbname = dbname;
		this.script = script;
		this.isIncre = isIncre;
		this.preSql = preSql;
		this.schedule = schedule;
		this.desc = desc;
		this.runTime = runTime;
		this.runHiveTimes = runHiveTimes;
		this.logPath = logPath;
		this.runHiveSuccess = runHiveSuccess;
		this.reRunMaxTimes = reRunMaxTimes;
		this.runSqoopSuccess = runSqoopSuccess;
	}

	@Override
	public String toString() {
		return "Task{" +
				"id='" + id + '\'' +
				", hive_id='" + hive_id + '\'' +
				", dbname='" + dbname + '\'' +
				", script='" + script + '\'' +
				", isIncre='" + isIncre + '\'' +
				", preSql='" + preSql + '\'' +
				", schedule='" + schedule + '\'' +
				", desc='" + desc + '\'' +
				", runTime='" + runTime + '\'' +
				", runHiveTimes=" + runHiveTimes +
				", logPath='" + logPath + '\'' +
				", runHiveSuccess=" + runHiveSuccess +
				", reRunMaxTimes=" + reRunMaxTimes +
				", runSqoopSuccess=" + runSqoopSuccess +
				'}';
	}
}
