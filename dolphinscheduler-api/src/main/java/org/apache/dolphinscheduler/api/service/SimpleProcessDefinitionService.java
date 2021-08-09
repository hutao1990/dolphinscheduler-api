package org.apache.dolphinscheduler.api.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.SneakyThrows;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.ProcessDefineTransfer;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.ScheduleMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author hutao
 * @date 2021/7/22 11:39
 * @description 简单工作流定义服务
 */
@Service
public class SimpleProcessDefinitionService extends BaseDAGService {

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private ProcessDefinitionMapper processDefineMapper;

    @Autowired
    private SchedulerService schedulerService;

    @Autowired
    private ScheduleMapper scheduleMapper;


    private static final String PROCESSDEFINITIONID = "processDefinitionId";

    /**
     * query process definition list paging
     *
     * @param loginUser   login user
     * @param projectName project name
     * @param searchVal   search value
     * @param pageNo      page number
     * @param pageSize    page size
     * @param userId      user id
     * @return process definition page
     */
    public Map<String, Object> queryProcessDefinitionListPaging(User loginUser, String projectName, String searchVal, Integer pageNo, Integer pageSize, Integer userId) {

        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }

        Page<ProcessDefinition> page = new Page(pageNo, pageSize);
        IPage<ProcessDefinition> processDefinitionIPage = processDefineMapper.queryDefineListPaging(
                page, searchVal, userId, project.getId(), isAdmin(loginUser), 1);

        List<ProcessDefinition> records = processDefinitionIPage.getRecords();

        List<SimpleProcessDefinition> collect = records.stream().map(ProcessDefineTransfer::toSimpleProcessDefinition).collect(Collectors.toList());

        PageInfo pageInfo = new PageInfo<ProcessData>(pageNo, pageSize);
        pageInfo.setTotalCount((int) processDefinitionIPage.getTotal());
        pageInfo.setLists(collect);
        result.put(Constants.DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    /**
     * 新增简单工作流
     *
     * @param loginUser     登录用户
     * @param projectName   项目名称
     * @param processName   工作流名称
     * @param cron          cron调度表达式
     * @param params        脚本参数
     * @param serialization 串行化开启标志
     * @param maxRetries    最大重试次数
     * @param mail          告警邮箱
     * @param phone         告警电话
     * @param timeout       超时时间
     * @return 工作流创建结果
     */
    @SneakyThrows
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> saveProcessDefine(User loginUser, String projectName, String processName, String cron, String params, int serialization, int maxRetries, String mail, String phone, int timeout) {
        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }
        String currentTime = DateUtils.getCurrentTime();
        SimpleProcessDefinition definition = new SimpleProcessDefinition();
        definition.setProjectId(project.getId());
        definition.setCreateTime(currentTime);
        definition.setUpdateTime(currentTime);
        definition.setSerialization(serialization);
        definition.setCron(cron);
        definition.setMail(mail);
        definition.setName(processName);
        definition.setParams(params);
        definition.setMaxRetries(maxRetries);
        definition.setModifyBy(loginUser.getUserName());
        definition.setPhone(phone);
        definition.setStatus(0);
        definition.setTimeout(timeout);
        definition.setEnableTimeout(timeout <= 0 ? 0 : 1);
        definition.setContent("#!/bin/bash  \necho '----------bash start----------'\n\necho '----------bash end----------'\nexit 0");

        ProcessDefinition processDefinition = ProcessDefineTransfer.toProcessDefinition(definition, loginUser.getTenantId(), "create");

        int i = processDefineMapper.insert(processDefinition);
        Map<String, Object> map = schedulerService.insertSchedule(loginUser, projectName,
                processDefinition.getId(),
                cron, WarningType.FAILURE, 0,
                FailureStrategy.CONTINUE,
                processDefinition.getReceivers(), null,
                Priority.MEDIUM, "default");
        Status s = (Status)map.get(Constants.STATUS);
        if (Status.SUCCESS.getCode() != s.getCode()){
            return map;
        }

        if (i > 0) {
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, processDefinition.getId());
            result.put("scheduleId",map.get("scheduleId"));
        } else {
            putMsg(result, Status.CREATE_PROCESS_DEFINITION);
        }
        return result;
    }

    /**
     * 新增简单工作流
     *
     * @param loginUser     登录用户
     * @param projectName   项目名称
     * @param id            工作流id
     * @param processName   工作流名称
     * @param cron          cron调度表达式
     * @param params        脚本参数
     * @param serialization 串行化开启标志
     * @param maxRetries    最大重试次数
     * @param mail          告警邮箱
     * @param phone         告警电话
     * @param timeout       超时时间
     * @param status        工作流状态
     * @return 工作流创建结果
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> updateProcessDefine(User loginUser, String projectName, int id, String processName, String cron, String params, int serialization, int maxRetries, String mail, String phone, int timeout, int status) {
        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }
        ProcessDefinition processDefinition = processDefineMapper.queryByDefineId(id);
        SimpleProcessDefinition definition = ProcessDefineTransfer.toSimpleProcessDefinition(processDefinition);
        definition.setUpdateTime(DateUtils.getCurrentTime());
        definition.setSerialization(serialization);
        definition.setCron(cron);
        definition.setMail(mail);
        definition.setName(processName);
        definition.setParams(params);
        definition.setMaxRetries(maxRetries);
        definition.setModifyBy(loginUser.getUserName());
        definition.setPhone(phone);
        definition.setStatus(status);
        definition.setTimeout(timeout);
        definition.setEnableTimeout(timeout <= 0 ? 0 : 1);

        ProcessDefinition processDefinition1 = ProcessDefineTransfer.toProcessDefinition(definition, loginUser.getTenantId(), "update");


        int i = processDefineMapper.updateById(processDefinition1);

        List<Schedule> schedules = scheduleMapper.queryByProcessDefinitionId(id);
        if (schedules.size() == 0){
            putMsg(result, Status.SIMPLE_PROCESS_UPDATE_ERROR,"工作流调度信息不存在！");
            result.put(PROCESSDEFINITIONID, processDefinition.getId());
            return  result;
        }
        Schedule schedule = schedules.get(0);
        schedule.setCrontab(cron);
        schedule.setUpdateTime(DateUtils.getCurrentDate());
        schedule.setReleaseState(status == 0? ReleaseState.OFFLINE:ReleaseState.ONLINE);
        scheduleMapper.updateById(schedule);
        if (i > 0) {
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, processDefinition.getId());
        } else {
            putMsg(result, Status.UPDATE_PROCESS_DEFINITION_ERROR);
            result.put(PROCESSDEFINITIONID, processDefinition.getId());
        }
        return result;
    }

    /**
     * 删除简单工作流
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return 工作流删除结果
     */
    public Map<String, Object> deleteById(User loginUser, String projectName, int id) {
        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }

        int i = processDefineMapper.deleteById(id);

        List<Schedule> schedules = scheduleMapper.queryByProcessDefinitionId(id);
        if (schedules.size() > 0) {
            schedules.forEach(s ->{
                scheduleMapper.deleteById(s.getId());
            });
        }
        if (i > 0) {
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, id);
        } else {
            putMsg(result, Status.DELETE_PROCESS_DEFINE_BY_ID_ERROR);
        }

        return result;
    }


    /**
     * 根据id查询简单工作流
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return 工作流查询结果
     */
    public Map<String, Object> selectById(User loginUser, String projectName, int id) {
        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }

        ProcessDefinition processDefinition = processDefineMapper.selectById(id);
        if (processDefinition == null) {
            putMsg(result, Status.PROCESS_INSTANCE_NOT_EXIST, id);
        } else {
            SimpleProcessDefinition definition = ProcessDefineTransfer.toSimpleProcessDefinition(processDefinition);
            List<Schedule> schedules = scheduleMapper.queryByProcessDefinitionId(definition.getId());
            if (schedules.size() == 0){
                putMsg(result, Status.SIMPLE_PROCESS_UPDATE_ERROR,"工作流调度信息不存在！");
                result.put(PROCESSDEFINITIONID, processDefinition.getId());
                return  result;
            }
            Schedule schedule = schedules.get(0);
            definition.setCron(schedule.getCrontab());
            definition.setStatus(schedule.getReleaseState().getCode());
            result.put(Constants.DATA_LIST, definition);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * 更新简单工作流脚本内容
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return 工作流更新结果
     */
    public Map<String, Object> updateContentById(User loginUser, String projectName, int id, String content) {
        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }

        ProcessDefinition processDefinition = processDefineMapper.selectById(id);
        SimpleProcessDefinition definition = ProcessDefineTransfer.toSimpleProcessDefinition(processDefinition);
        definition.setContent(content);
        ProcessDefinition definition1 = ProcessDefineTransfer.toProcessDefinition(definition,loginUser.getTenantId(),"update");
        int i = processDefineMapper.updateById(definition1);
        if (i > 0) {
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, id);
        } else {
            putMsg(result, Status.UPDATE_PROCESS_DEFINITION_ERROR);
        }

        return result;
    }


    /**
     * 更新简单工作流上线
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return 工作流更新结果
     */
    public Map<String, Object> online(User loginUser, String projectName, int id) {
        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }

        ProcessDefinition processDefinition = processDefineMapper.selectById(id);
        SimpleProcessDefinition definition = ProcessDefineTransfer.toSimpleProcessDefinition(processDefinition);
        definition.setStatus(1);
        ProcessDefinition definition1 = ProcessDefineTransfer.toProcessDefinition(definition,loginUser.getTenantId(),"update");
        int i = processDefineMapper.updateById(definition1);
        List<Schedule> schedules = scheduleMapper.queryByProcessDefinitionId(id);
        if (schedules.size() == 0){
            putMsg(result, Status.SIMPLE_PROCESS_UPDATE_ERROR,"工作流调度信息不存在！");
            result.put(PROCESSDEFINITIONID, processDefinition.getId());
            return  result;
        }
        Schedule schedule = schedules.get(0);
        schedule.setUpdateTime(DateUtils.getCurrentDate());
        schedule.setReleaseState(ReleaseState.ONLINE);
        scheduleMapper.updateById(schedule);
        if (i > 0) {
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, id);
        } else {
            putMsg(result, Status.PUBLISH_SCHEDULE_ONLINE_ERROR);
        }

        return result;
    }


    /**
     * 更新简单工作流下线
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return 工作流更新结果
     */
    public Map<String, Object> offline(User loginUser, String projectName, int id) {
        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }

        ProcessDefinition processDefinition = processDefineMapper.selectById(id);
        SimpleProcessDefinition definition = ProcessDefineTransfer.toSimpleProcessDefinition(processDefinition);
        definition.setStatus(0);
        ProcessDefinition definition1 = ProcessDefineTransfer.toProcessDefinition(definition,loginUser.getTenantId(),"update");
        int i = processDefineMapper.updateById(definition1);
        List<Schedule> schedules = scheduleMapper.queryByProcessDefinitionId(id);
        if (schedules.size() == 0){
            putMsg(result, Status.SIMPLE_PROCESS_UPDATE_ERROR,"工作流调度信息不存在！");
            result.put(PROCESSDEFINITIONID, processDefinition.getId());
            return  result;
        }
        Schedule schedule = schedules.get(0);
        schedule.setUpdateTime(DateUtils.getCurrentDate());
        schedule.setReleaseState(ReleaseState.OFFLINE);
        scheduleMapper.updateById(schedule);
        if (i > 0) {
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, id);
        } else {
            putMsg(result, Status.PUBLISH_SCHEDULE_ONLINE_ERROR);
        }

        return result;
    }
}
