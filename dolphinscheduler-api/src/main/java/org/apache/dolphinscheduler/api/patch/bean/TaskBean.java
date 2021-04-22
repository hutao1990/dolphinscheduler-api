package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Data;

import java.util.List;

/**
 * @author hutao
 * @date 2021/3/18 19:45
 * @description
 */
@Data
public class TaskBean {
    private boolean phoneAlarmEnable;
    private ConditionResultBean conditionResult;
    private String description;
    private String runFlag;
    private String type;
    private ParamsBean params;
    private TimeoutBean timeout;
    private String maxRetryTimes;
    private String taskInstancePriority;
    private String name;
    private DependenceBean dependence;
    private String retryInterval;
    private boolean mailAlarmEnable;
    private List<String> preTasks;
    private String id;
    private String workerGroup;
}
