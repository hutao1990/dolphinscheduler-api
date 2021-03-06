/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.server.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gome.hkalarm.sdk.bean.PhoneBean;
import com.gome.hkalarm.sdk.util.SDK;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import javafx.concurrent.Task;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.DaoFactory;
import org.apache.dolphinscheduler.dao.datasource.ConnectionFactory;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * alert manager
 */
public class AlertManager {

    /**
     * logger of AlertManager
     */
    private static final Logger logger = LoggerFactory.getLogger(AlertManager.class);

    /**
     * alert dao
     */
    private AlertDao alertDao = DaoFactory.getDaoInstance(AlertDao.class);


    private UserMapper userMapper = ConnectionFactory.getInstance().getMapper(UserMapper.class);


    public static Cache<Integer, List<Integer>> cache = CacheBuilder.newBuilder()
            .initialCapacity(128)
            .expireAfterWrite(18, TimeUnit.HOURS)
            .build();

    public static Cache<Integer, List<Integer>> phCache = CacheBuilder.newBuilder()
            .initialCapacity(128)
            .expireAfterWrite(18, TimeUnit.HOURS)
            .build();

    /**
     * command type convert chinese
     *
     * @param commandType command type
     * @return command name
     */
    private String getCommandCnName(CommandType commandType) {
        switch (commandType) {
            case RECOVER_TOLERANCE_FAULT_PROCESS:
                return "recover tolerance fault process";
            case RECOVER_SUSPENDED_PROCESS:
                return "recover suspended process";
            case START_CURRENT_TASK_PROCESS:
                return "start current task process";
            case START_FAILURE_TASK_PROCESS:
                return "start failure task process";
            case START_PROCESS:
                return "start process";
            case REPEAT_RUNNING:
                return "repeat running";
            case SCHEDULER:
                return "scheduler";
            case COMPLEMENT_DATA:
                return "complement data";
            case PAUSE:
                return "pause";
            case STOP:
                return "stop";
            default:
                return "unknown type";
        }
    }

    /**
     * process instance format
     */
    private static final String PROCESS_INSTANCE_FORMAT =
            "\"id:%d\"," +
                    "\"name:%s\"," +
                    "\"job type: %s\"," +
                    "\"state: %s\"," +
                    "\"recovery:%s\"," +
                    "\"run time: %d\"," +
                    "\"start time: %s\"," +
                    "\"end time: %s\"," +
                    "\"serialization: %s\"," +
                    "\"host: %s\"";

    /**
     * get process instance content
     *
     * @param processInstance process instance
     * @param taskInstances   task instance list
     * @return process instance format content
     */
    public String getContentProcessInstance(ProcessInstance processInstance,
                                            List<TaskInstance> taskInstances) {

        String res = "";
        logger.info("====> task instance num: {}", taskInstances.size());
        if (processInstance.getState().typeIsSuccess() || taskInstances.size() == 0) {
            res = String.format(PROCESS_INSTANCE_FORMAT,
                    processInstance.getId(),
                    processInstance.getName(),
                    getCommandCnName(processInstance.getCommandType()),
                    processInstance.getState().toString(),
                    processInstance.getRecovery().toString(),
                    processInstance.getRunTimes(),
                    DateUtils.dateToString(processInstance.getStartTime()),
                    DateUtils.dateToString(processInstance.getEndTime()),
                    processInstance.getProcessDefinition().getSerialization(),
                    processInstance.getHost()

            );
            res = "[" + res + "]";
        } else {

            List<LinkedHashMap> failedTaskList = new ArrayList<>();
            for (TaskInstance task : taskInstances) {
                if (!task.getState().typeIsFailure()) {
                    continue;
                }
                if (!sendMail(Collections.singletonList(task))){
                    logger.info("process '{}' task '{}' has close mail alarm??? ignore this alarm???",processInstance.getName(),task.getName());
                    continue;
                }
                List<Integer> list = new ArrayList<>();
                try {
                    list = cache.get(task.getProcessInstanceId(), ArrayList::new);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                if (!list.isEmpty() && list.contains(task.getId())) {
                    continue;
                }
                list.add(task.getId());
                cache.put(task.getProcessInstanceId(), list);
                LinkedHashMap<String, String> failedTaskMap = new LinkedHashMap();
                failedTaskMap.put("process instance id", String.valueOf(processInstance.getId()));
                failedTaskMap.put("process instance name", processInstance.getName());
                failedTaskMap.put("task id", String.valueOf(task.getId()));
                failedTaskMap.put("task name", task.getName());
                failedTaskMap.put("task type", task.getTaskType());
                failedTaskMap.put("task state", task.getState().toString());
                failedTaskMap.put("task start time", DateUtils.dateToString(task.getStartTime()));
                failedTaskMap.put("task end time", task.getEndTime() != null ? DateUtils.dateToString(task.getEndTime()) : "");
                failedTaskMap.put("host", task.getHost());
                failedTaskMap.put("log path", task.getLogPath());
                failedTaskList.add(failedTaskMap);
            }
            res = JSONUtils.toJson(failedTaskList);
        }

        return res;
    }

    /**
     * getting worker fault tolerant content
     *
     * @param processInstance   process instance
     * @param toleranceTaskList tolerance task list
     * @return worker tolerance content
     */
    private String getWorkerToleranceContent(ProcessInstance processInstance, List<TaskInstance> toleranceTaskList) {

        List<LinkedHashMap<String, String>> toleranceTaskInstanceList = new ArrayList<>();

        for (TaskInstance taskInstance : toleranceTaskList) {
            if (!sendMail(Collections.singletonList(taskInstance))){
                logger.info("process '{}' task '{}' has close mail alarm??? ignore this alarm???",processInstance.getName(),taskInstance.getName());
                continue;
            }
            LinkedHashMap<String, String> toleranceWorkerContentMap = new LinkedHashMap();
            toleranceWorkerContentMap.put("process name", processInstance.getName());
            toleranceWorkerContentMap.put("task name", taskInstance.getName());
            toleranceWorkerContentMap.put("host", taskInstance.getHost());
            toleranceWorkerContentMap.put("task retry times", String.valueOf(taskInstance.getRetryTimes()));
            toleranceTaskInstanceList.add(toleranceWorkerContentMap);
        }
        return JSONUtils.toJson(toleranceTaskInstanceList);
    }

    /**
     * send worker alert fault tolerance
     *
     * @param processInstance   process instance
     * @param toleranceTaskList tolerance task list
     */
    public void sendAlertWorkerToleranceFault(ProcessInstance processInstance, List<TaskInstance> toleranceTaskList) {
        try {
            Alert alert = new Alert();
            alert.setTitle("worker fault tolerance");
            alert.setShowType(ShowType.TABLE);
            String content = getWorkerToleranceContent(processInstance, toleranceTaskList);
            if (content == null || content.trim().length() < 8) {
                logger.info("ignore blank alert...");
                return;
            }
            alert.setContent(content);
            alert.setAlertType(AlertType.EMAIL);
            alert.setCreateTime(new Date());
            alert.setAlertGroupId(processInstance.getWarningGroupId() == null ? 1 : processInstance.getWarningGroupId());
            alert.setReceivers(processInstance.getProcessDefinition().getReceivers());
            alert.setReceiversCc(processInstance.getProcessDefinition().getReceiversCc());
            alertDao.addAlert(alert);
            logger.info("add alert to db , alert : {}", alert.toString());

        } catch (Exception e) {
            logger.error("send alert failed:{} ", e.getMessage());
        }

    }

    /**
     * send process instance alert
     *
     * @param processInstance process instance
     * @param taskInstances   task instance list
     */
    public void sendAlertProcessInstance(ProcessInstance processInstance,
                                         List<TaskInstance> taskInstances) {
        if (Flag.YES == processInstance.getIsSubProcess()) {
            return;
        }

        boolean sendWarnning = false;
        WarningType warningType = processInstance.getWarningType();
        switch (warningType) {
            case ALL:
                if (processInstance.getState().typeIsFinished()) {
                    sendWarnning = true;
                }
                break;
            case SUCCESS:
                if (processInstance.getState().typeIsSuccess()) {
                    sendWarnning = true;
                }
                break;
            case FAILURE:
                if (processInstance.getState().typeIsFailure() || processInstance.getState().typeIsStop()) {
                    sendWarnning = true;
                }
                break;
            default:
        }
        // ????????????process???task error???????????????
        if (!processInstance.getState().typeIsFinished()) {
            for (TaskInstance task : taskInstances) {
                if (task.getState().typeIsFailure()) {
                    logger.error(" ----> send mail notice!");
                    sendWarnning = true;
                    break;
                }
            }
        }
        if (!sendWarnning) {
            return;
        }
        Alert alert = new Alert();


        String cmdName = getCommandCnName(processInstance.getCommandType());
        String success = processInstance.getState().typeIsSuccess() ? "success" : "failed";
        alert.setTitle(cmdName + " " + success);
        ShowType showType = processInstance.getState().typeIsSuccess() ? ShowType.TEXT : ShowType.TABLE;
        if (taskInstances.isEmpty()) {
            showType = ShowType.TEXT;
        }
        alert.setShowType(showType);
        String content = getContentProcessInstance(processInstance, taskInstances);
        if (content != null && content.trim().length() >= 8) {
            alert.setContent(content);
            alert.setAlertType(AlertType.EMAIL);
            alert.setAlertGroupId(processInstance.getWarningGroupId());
            alert.setCreateTime(new Date());
            alert.setReceivers(processInstance.getProcessDefinition().getReceivers());
            alert.setReceiversCc(processInstance.getProcessDefinition().getReceiversCc());

            alertDao.addAlert(alert);
            logger.info("add alert to db , alert: {}", alert.toString());
        }else {
            logger.info("content is blank, ignore mail alert!");
        }

        try {
            List<Integer> list = phCache.get(processInstance.getId(), ArrayList::new);
            List<TaskInstance> collect = new ArrayList<>();
            Map<String, List<TaskInstance>> alaramMap = new HashMap<>();
            User user = userMapper.selectById(processInstance.getProcessDefinition().getUserId());
            if (!taskInstances.isEmpty()) {
                collect = taskInstances.stream().filter(t -> Arrays.asList(5, 6, 8, 9).contains(t.getState().getCode())).filter(t -> !list.contains(t.getId())).collect(Collectors.toList());
                alaramMap = callPhone(collect, user.getPhone(), processInstance.getSimple());
            }else {
                String definitionJson = processInstance.getProcessDefinition().getProcessDefinitionJson();
                ProcessData processData = JSON.parseObject(definitionJson, ProcessData.class);
                List<TaskNode> tasks = processData.getTasks();
                if (tasks.size() > 0){
                    String phoneNumber = tasks.get(0).getPhoneNumber();
                    if (StringUtils.isNotBlank(phoneNumber)){
                        alaramMap.put(phoneNumber,new ArrayList<>());
                    }
                }
            }
            if (!alaramMap.isEmpty()) {
                alaramMap.forEach((phone, tasks) -> {
                    logger.info("callPhone " + phone + " alarm!");
                    PhoneBean phoneBean = new PhoneBean();
                    phoneBean.setAppId("dolphinscheduler");
                    phoneBean.setDetailId(user.getUserName() + "#" + processInstance.getProcessDefinition().getName());
                    phoneBean.setPhoneNumber(phone);
                    phoneBean.setTitle("scheduler alarm");
                    if (processInstance.getSimple() == 0) {
                        phoneBean.setContent("process: " + processInstance.getName() + ", task:" + StringUtils.join(tasks.stream().map(TaskInstance::getName).collect(Collectors.toList()), ",") + " error!");
                        list.addAll(tasks.stream().map(TaskInstance::getId).collect(Collectors.toList()));
                    } else {
                        phoneBean.setContent("process: " + processInstance.getName() + " error!");
                    }
                    SDK.HkAlarmSDK.getInstance().sendPhoneMsg(phoneBean);
                });
            }
        }catch (Exception e) {
            e.printStackTrace();
            logger.error("cache get error! phone msg send error! e: {}",e.getMessage());
        }

    }

    private Map<String, List<TaskInstance>> callPhone(List<TaskInstance> taskInstances,String phone, int simple) {
        Map<String, List<TaskInstance>> alarmMap = new HashMap<>();
        if (taskInstances != null && !taskInstances.isEmpty()) {
            for (TaskInstance task : taskInstances) {
                String str = task.getTaskJson();
                JSONObject json = JSONObject.parseObject(str);
                boolean phoneAlarmEnable = json.getBooleanValue("phoneAlarmEnable");
                String ph = json.getOrDefault("phoneNumber","").toString();
                if (phoneAlarmEnable || (StringUtils.isNotBlank(ph) && simple == 1)) {
                    String phoneStrategy = json.getOrDefault("phoneStrategy","default").toString();
                    String phoneNumber = json.getOrDefault("phoneNumber",phone).toString();
                    switch (phoneStrategy){
                        case "last_retry":
                            if (task.getRetryTimes() == task.getMaxRetryTimes()){
                                logger.info("task '{}({})' retry times is {}/{} strategy {}, start call phone!",task.getName(),task.getId(),task.getRetryTimes(),task.getMaxRetryTimes(),phoneStrategy);
                                if(alarmMap.containsKey(phoneNumber)){
                                    alarmMap.get(phoneNumber).add(task);
                                }else {
                                    List<TaskInstance> tasks = new ArrayList<>();
                                    tasks.add(task);
                                    alarmMap.put(phoneNumber,tasks);
                                }
                            }else {
                                logger.info("task '{}({})' retry times is {}/{} strategy {}, skip call phone!",task.getName(),task.getId(),task.getRetryTimes(),task.getMaxRetryTimes(),phoneStrategy);
                                break;
                            }
                        case "default":
                        default:
                            logger.info("task '{}({})' retry times is {}/{} strategy {}, start call phone!",task.getName(),task.getId(),task.getRetryTimes(),task.getMaxRetryTimes(),phoneStrategy);
                            if(alarmMap.containsKey(phoneNumber)){
                                alarmMap.get(phoneNumber).add(task);
                            }else {
                                List<TaskInstance> tasks = new ArrayList<>();
                                tasks.add(task);
                                alarmMap.put(phoneNumber,tasks);
                            }
                    }
                }
            }
        }
        return alarmMap;
    }
    private boolean sendMail(List<TaskInstance> taskInstances) {
        if (taskInstances != null && !taskInstances.isEmpty()) {
            for (TaskInstance task : taskInstances) {
                String str = task.getTaskJson();
                JSONObject json = JSONObject.parseObject(str);
                boolean mailAlarmEnable = json.getBooleanValue("mailAlarmEnable");
                if (mailAlarmEnable) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * send process timeout alert
     *
     * @param processInstance   process instance
     * @param processDefinition process definition
     */
    public void sendProcessTimeoutAlert(ProcessInstance processInstance, ProcessDefinition processDefinition) {
        alertDao.sendProcessTimeoutAlert(processInstance, processDefinition);
    }
}
