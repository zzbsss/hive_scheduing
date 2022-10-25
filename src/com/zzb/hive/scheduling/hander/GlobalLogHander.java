package com.zzb.hive.scheduling.hander;

/**
 * 全局日志处理
 */
public interface GlobalLogHander {
    void writeGlobalLog(String message,boolean isErrorLog);
}
