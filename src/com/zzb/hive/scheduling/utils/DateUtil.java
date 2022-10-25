package com.zzb.hive.scheduling.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    /**
     * 拿到当前日期的MMdd
     * @return
     */
    public static String getCurrMonDay(){
        SimpleDateFormat sdf = new SimpleDateFormat("MMdd");
        return  sdf.format(new Date());
    }

    /**
     * 拿到当前日期的 dd
     * @return
     */
    public  static String getCurrDay(){
        return  getCurrMonDay().substring(2,4);
    }

    /**
     *获取当前星期
     * @return
     */
    public static String getWeekDay() {
        String [] weekDays = {"周日","周一","周二","周三","周四","周五","周六"};
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        int w = cal.get(Calendar.DAY_OF_WEEK)-1;
        if(w<0)
            w=0;
        return weekDays[w];
    }
}
