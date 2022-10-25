package com.zzb.hive.scheduling.service.impl;

import com.zzb.hive.scheduling.action.Hive2DbAction;
import com.zzb.hive.scheduling.hander.GlobalLogHander;

public class GlobalLogImpl implements GlobalLogHander {
    @Override
    public void writeGlobalLog(String message, boolean isErrorLog) {
        Hive2DbAction.logMsg(Hive2DbAction.getLogFile(isErrorLog?"error":"success"),message);
    }
}
