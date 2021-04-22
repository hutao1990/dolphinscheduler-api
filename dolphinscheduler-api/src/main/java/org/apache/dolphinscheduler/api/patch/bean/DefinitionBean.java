package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Data;

import java.util.List;

/**
 * @author hutao
 * @date 2021/3/18 19:40
 * @description
 */
@Data
public class DefinitionBean {
    private int tenantId;
    private List<GlobalParamsBean> globalParams;
    private List<TaskBean> tasks;
    private int timeout;
}
