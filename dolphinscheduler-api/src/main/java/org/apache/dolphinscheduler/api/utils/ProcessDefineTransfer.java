package org.apache.dolphinscheduler.api.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessData;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.SimpleProcessDefinition;

import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author hutao
 * @date 2021/7/27 11:19
 * @description
 */
public abstract class ProcessDefineTransfer {


    /**
     * 转换为简单任务
     * @param process
     * @return
     */
    public static SimpleProcessDefinition toSimpleProcessDefinition(ProcessDefinition process) {
        SimpleProcessDefinition definition = new SimpleProcessDefinition();
        ProcessData processData = JSONUtils.parseObject(process.getProcessDefinitionJson(), ProcessData.class);



        definition.setId(process.getId());
        definition.setProjectId(process.getProjectId());
        definition.setName(process.getName());
        definition.setCron(process.getScheduleCrontab());
        definition.setParams(process.getShellParams());
        definition.setSerialization("1".equals(process.getSerialization())? Integer.parseInt(process.getSerialization()) : 0);
        definition.setMail(process.getReceivers());
        if (process.getTimeout() >= 3600 * 24 || process.getTimeout() <= 0){
            definition.setEnableTimeout(0);
        }else {
            definition.setEnableTimeout(1);
        }
        definition.setTimeout(process.getTimeout());
        definition.setStatus(process.getScheduleReleaseState() == ReleaseState.ONLINE ? 1 : 0);
        definition.setCreateTime(DateUtils.dateToString(process.getCreateTime()));
        definition.setUpdateTime(DateUtils.dateToString(process.getUpdateTime()));
        definition.setModifyBy(process.getModifyBy());

        if (processData != null) {
            List<TaskNode> tasks = processData.getTasks();
            if (tasks != null && tasks.size() > 0) {
                TaskNode taskNode = tasks.get(0);
                JSONObject params = JSON.parseObject(taskNode.getParams());
                definition.setPhone(taskNode.getPhoneNumber());
                definition.setMaxRetries(taskNode.getMaxRetryTimes());
                definition.setContent(params.getString("rawScript"));
            }
        }
        return definition;
    }

    /**
     * 转换为普通任务
     * @param definition
     * @param userId
     * @return
     */
    public static ProcessDefinition toProcessDefinition(SimpleProcessDefinition definition, int userId,String opt){
        ProcessDefinition process = new ProcessDefinition();

        JSONObject jsonObject = new JSONObject(true);
        jsonObject.put("resourceList",new JSONArray());
        jsonObject.put("localParams",new JSONArray());
        jsonObject.put("rawScript",definition.getContent());
        TaskNode taskNode = new TaskNode();
        taskNode.setPhoneNumber(definition.getPhone());
        taskNode.setName(Constants.SIMPLE_TASK_NAME);
        taskNode.setId(Constants.SIMPLE_TASK_NAME);
        taskNode.setParams(jsonObject.toJSONString());
        taskNode.setDesc(definition.getName());
        taskNode.setMailAlarmEnable(true);
        taskNode.setPhoneAlarmEnable(StringUtils.isNotBlank(definition.getPhone()));
        taskNode.setMaxRetryTimes(definition.getMaxRetries());
        taskNode.setRunFlag("NORMAL");
        taskNode.setTaskInstancePriority(Priority.MEDIUM);
        taskNode.setWorkerGroup("default");
        taskNode.setType("SHELL");

        JSONObject locations = new JSONObject(true);
        JSONObject loc = new JSONObject(true);
        loc.put("name",Constants.SIMPLE_TASK_NAME);
        loc.put("targetarr","");
        loc.put("nodenumber","0");
        loc.put("x",200);
        loc.put("y",200);
        locations.put(Constants.SIMPLE_TASK_NAME,loc);

        ProcessData processData = new ProcessData();
        processData.setSerialization(definition.getSerialization()+"");
        if (definition.getEnableTimeout() == 1 && definition.getTimeout() > 0) {
            processData.setTimeout(definition.getTimeout());
            process.setTimeout(definition.getTimeout());
        }else {
            processData.setTimeout(3600 * 24);
            process.setTimeout(3600 * 24);
        }
        processData.setTenantId(userId);
        processData.setTasks(Collections.singletonList(taskNode));

        process.setName(definition.getName());
        process.setReleaseState(ReleaseState.getEnum(definition.getStatus()));
        process.setProjectId(definition.getProjectId());
        process.setUserId(userId);
        process.setProcessDefinitionJson(JSON.toJSONString(processData));
        process.setDescription(definition.getName());
        process.setLocations(locations.toJSONString());
        process.setConnects(new JSONArray().toJSONString());
        process.setTenantId(userId);
        process.setModifyBy(definition.getModifyBy());
        process.setReceivers(definition.getMail());
//        process.setResourceIds(getResourceIds(processData));
        process.setSerialization(definition.getSerialization()+"");
        process.setShellParams(definition.getParams());

        Date now = DateUtils.getCurrentDate();
        if ("create".equals(opt)) {
            process.setCreateTime(now);
        }else {
            process.setId(definition.getId());
        }
        process.setUpdateTime(now);
        process.setFlag(Flag.YES);
        process.setSimple(1);


        return process;
    }

}
