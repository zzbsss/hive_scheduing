package com.zzb.hive.scheduling.service;

import com.zzb.hive.scheduling.vo.Task;

import java.util.List;

public interface ScriptService {

   void run(List<Task> tasks);

   void run(Task task);
}
