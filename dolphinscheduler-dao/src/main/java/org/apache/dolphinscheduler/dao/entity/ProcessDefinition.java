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
package org.apache.dolphinscheduler.dao.entity;

import com.alibaba.fastjson.JSON;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.process.Property;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * process definition
 */
@TableName("t_ds_process_definition")
public class ProcessDefinition {
    /**
     * id
     */
    @TableId(value="id", type=IdType.AUTO)
    private int id;

    /**
     * name
     */
    private String name;

    /**
     * version
     */
    private int version;

    /**
     * release state : online/offline
     */
    private ReleaseState releaseState;

    /**
     * project id
     */
    private int projectId;

    /**
     * definition json string
     */
    private String processDefinitionJson;

    /**
     * description
     */
    private String description;

    /**
     * user defined parameters
     */
    private String globalParams;

    /**
     * user defined parameter list
     */
    @TableField(exist=false)
    private List<Property> globalParamList;

    /**
     * user define parameter map
     */
    @TableField(exist=false)
    private Map<String,String> globalParamMap;

    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;

    /**
     * process is valid: yes/no
     */
    private Flag flag;

    /**
     * process user id
     */
    private int userId;

    /**
     * user name
     */
    @TableField(exist = false)
    private String userName;

    /**
     * project name
     */
    @TableField(exist = false)
    private String projectName;

    /**
     * locations array for web
     */
    private String locations;

    /**
     * connects array for web
     */
    private String connects;

    /**
     * receivers
     */
    private String receivers;

    /**
     * receivers cc
     */
    private String receiversCc;

    /**
     * schedule id
     */
    @TableField(exist=false)
    private int scheduleId;

    /**
     * schedule release state : online/offline
     */
    @TableField(exist=false)
    private ReleaseState scheduleReleaseState;

    /**
     * crontab expression
     */
    @TableField(exist = false)
    private String scheduleCrontab;

    /**
     * failure strategy
     */
    @TableField(exist = false)
    private FailureStrategy scheduleFailureStrategy;

    /**
     * create time
     */
    @TableField(exist = false)
    private Date scheduleCreateTime;

    /**
     * update time
     */
    @TableField(exist = false)
    private Date scheduleUpdateTime;

    /**
     * schedule start time
     */
    @TableField(exist = false)
    private Date scheduleStartTime;

    /**
     * schedule end time
     */
    @TableField(exist = false)
    private Date scheduleEndTime;

    /**
     * shell param
     */
    private String shellParams;

    /**
     * process warning time out. unit: minute
     */
    private int timeout;

    /**
     * tenant id
     */
    private int tenantId;

    /**
     * modify user name
     */
    private String modifyBy;

    /**
     * resource ids
     */
    private String resourceIds;

    /**
     * serialization enable
     */
    private String serialization;

    /**
     * ?????????????????? 1???????????????  0??????????????????
     */
    private int simple;

    public void setSerialization(String serialization){
        this.serialization = serialization;
    }

    public String getSerialization(){
        return serialization;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public ReleaseState getReleaseState() {
        return releaseState;
    }

    public void setReleaseState(ReleaseState releaseState) {
        this.releaseState = releaseState;
    }

    public String getProcessDefinitionJson() {
        return processDefinitionJson;
    }

    public void setProcessDefinitionJson(String processDefinitionJson) {
        this.processDefinitionJson = processDefinitionJson;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Flag getFlag() {
        return flag;
    }

    public void setFlag(Flag flag) {
        this.flag = flag;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }


    public String getGlobalParams() {
        return globalParams;
    }

    public void setGlobalParams(String globalParams) {
        this.globalParamList = JSON.parseArray(globalParams, Property.class);
        this.globalParams = globalParams;
    }

    public List<Property> getGlobalParamList() {
        return globalParamList;
    }

    public void setGlobalParamList(List<Property> globalParamList) {
        this.globalParams = JSON.toJSONString(globalParamList);
        this.globalParamList = globalParamList;
    }

    public Map<String, String> getGlobalParamMap() {
        List<Property> propList;

        if (globalParamMap == null && StringUtils.isNotEmpty(globalParams)) {
            propList = JSON.parseArray(globalParams, Property.class);
            if (propList != null) {
                globalParamMap = propList.stream().collect(Collectors.toMap(Property::getProp, Property::getValue));
            }
        }

        return globalParamMap;
    }

    public void setGlobalParamMap(Map<String, String> globalParamMap) {
        this.globalParamMap = globalParamMap;
    }

    public String getLocations() {
        return locations;
    }

    public void setLocations(String locations) {
        this.locations = locations;
    }

    public String getConnects() {
        return connects;
    }

    public void setConnects(String connects) {
        this.connects = connects;
    }

    public String getReceivers() {
        return receivers;
    }

    public void setReceivers(String receivers) {
        this.receivers = receivers;
    }

    public String getReceiversCc() {
        return receiversCc;
    }

    public void setReceiversCc(String receiversCc) {
        this.receiversCc = receiversCc;
    }

    public ReleaseState getScheduleReleaseState() {
        return scheduleReleaseState;
    }

    public void setScheduleReleaseState(ReleaseState scheduleReleaseState) {
        this.scheduleReleaseState = scheduleReleaseState;
    }

    public String getResourceIds() {
        return resourceIds;
    }

    public void setResourceIds(String resourceIds) {
        this.resourceIds = resourceIds;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getTenantId() {
        return tenantId;
    }

    public void setTenantId(int tenantId) {
        this.tenantId = tenantId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getModifyBy() {
        return modifyBy;
    }

    public void setModifyBy(String modifyBy) {
        this.modifyBy = modifyBy;
    }

    public String getScheduleCrontab() {
        return scheduleCrontab;
    }

    public void setScheduleCrontab(String scheduleCrontab) {
        this.scheduleCrontab = scheduleCrontab;
    }

    public FailureStrategy getScheduleFailureStrategy() {
        return scheduleFailureStrategy;
    }

    public void setScheduleFailureStrategy(FailureStrategy scheduleFailureStrategy) {
        this.scheduleFailureStrategy = scheduleFailureStrategy;
    }

    public Date getScheduleCreateTime() {
        return scheduleCreateTime;
    }

    public void setScheduleCreateTime(Date scheduleCreateTime) {
        this.scheduleCreateTime = scheduleCreateTime;
    }

    public Date getScheduleUpdateTime() {
        return scheduleUpdateTime;
    }

    public void setScheduleUpdateTime(Date scheduleUpdateTime) {
        this.scheduleUpdateTime = scheduleUpdateTime;
    }

    public Date getScheduleStartTime() {
        return scheduleStartTime;
    }

    public void setScheduleStartTime(Date scheduleStartTime) {
        this.scheduleStartTime = scheduleStartTime;
    }

    public Date getScheduleEndTime() {
        return scheduleEndTime;
    }

    public void setScheduleEndTime(Date scheduleEndTime) {
        this.scheduleEndTime = scheduleEndTime;
    }



    public String getShellParams() {
        return shellParams;
    }

    public void setShellParams(String shellParams) {
        this.shellParams = shellParams;
    }

    public int getSimple() {
        return simple;
    }

    public void setSimple(int simple) {
        this.simple = simple;
    }

    public int getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(int scheduleId) {
        this.scheduleId = scheduleId;
    }

    @Override
    public String toString() {
        return "ProcessDefinition{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", version=" + version +
                ", releaseState=" + releaseState +
                ", projectId=" + projectId +
                ", processDefinitionJson='" + processDefinitionJson + '\'' +
                ", description='" + description + '\'' +
                ", globalParams='" + globalParams + '\'' +
                ", globalParamList=" + globalParamList +
                ", globalParamMap=" + globalParamMap +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", flag=" + flag +
                ", userId=" + userId +
                ", userName='" + userName + '\'' +
                ", projectName='" + projectName + '\'' +
                ", locations='" + locations + '\'' +
                ", connects='" + connects + '\'' +
                ", receivers='" + receivers + '\'' +
                ", receiversCc='" + receiversCc + '\'' +
                ", scheduleReleaseState=" + scheduleReleaseState +
                ", timeout=" + timeout +
                ", tenantId=" + tenantId +
                ", modifyBy='" + modifyBy + '\'' +
                ", resourceIds='" + resourceIds + '\'' +
                '}';
    }

}
