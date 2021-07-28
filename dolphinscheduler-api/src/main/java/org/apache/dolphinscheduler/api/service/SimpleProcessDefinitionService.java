package org.apache.dolphinscheduler.api.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.ProcessDefineTransfer;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

        ProcessDefinition processDefinition = ProcessDefineTransfer.toProcessDefinition(definition, loginUser.getTenantId());

        processDefineMapper.insert(processDefinition);
        putMsg(result, Status.SUCCESS);
        result.put(PROCESSDEFINITIONID, processDefinition.getId());
        return result;
    }

}
