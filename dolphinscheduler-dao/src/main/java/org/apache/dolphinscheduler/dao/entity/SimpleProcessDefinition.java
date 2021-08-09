package org.apache.dolphinscheduler.dao.entity;

/**
 * @author hutao
 * @date 2021/7/23 18:20
 * @description
 */
public class SimpleProcessDefinition {

    //工作流id
    private int id;
    //项目id
    private int projectId;
    //工作流名称
    private String processName;
    //调度表达式
    private String cron;
    //参数
    private String params;
    //串行化开关
    private int serialization;
    //最大重试次数
    private int maxRetries;
    //告警邮箱，多个以逗号分给
    private String mail;
    //告警电话，多个以逗号分隔
    private String phone;
    //启用超时
    private int enableTimeout;
    //超时时间 分
    private int timeout;
    //状态
    private int status;
    //脚本内容
    private String content;
    //创建时间
    private String createTime;
    //更新时间
    private String updateTime;
    //修改用户
    private String modifyBy;


    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getModifyBy() {
        return modifyBy;
    }

    public void setModifyBy(String modifyBy) {
        this.modifyBy = modifyBy;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public int getSerialization() {
        return serialization;
    }

    public void setSerialization(int serialization) {
        this.serialization = serialization;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public String getMail() {
        return mail;
    }

    public void setMail(String mail) {
        this.mail = mail;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getEnableTimeout() {
        return enableTimeout;
    }

    public void setEnableTimeout(int enableTimeout) {
        this.enableTimeout = enableTimeout;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
