package org.apache.dolphinscheduler.api.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.dolphinscheduler.api.dto.ProcessMeta;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessData;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.SimpleProcessDefinition;

import java.util.List;

/**
 * @author hutao
 * @date 2021/7/27 11:19
 * @description
 */
public abstract class ProcessDefineTransfer {

    public static SimpleProcessDefinition toSimpleProcessDefinition(ProcessDefinition process) {
        SimpleProcessDefinition definition = new SimpleProcessDefinition();
        ProcessData processData = JSONUtils.parseObject(process.getProcessDefinitionJson(), ProcessData.class);



        definition.setId(process.getId());
        definition.setProjectId(process.getProjectId());
        definition.setName(process.getName());
        definition.setCron(process.getScheduleCrontab());
        definition.setParams(process.getShellParams());
        definition.setSerialization(process.getSerialization() == null ? 0 : Integer.parseInt(process.getSerialization()));
        definition.setMail(process.getReceivers());
        definition.setEnableTimeout(process.getTimeout() > 0 ? 1 : 0);
        definition.setTimeout(process.getTimeout());
        definition.setStatus(process.getScheduleReleaseState() == ReleaseState.ONLINE ? 1 : 0);
        definition.setCreateTime(DateUtils.dateToString(process.getCreateTime()));
        definition.setUpdateTime(DateUtils.dateToString(process.getUpdateTime()));

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
}
