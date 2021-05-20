package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Data;

/**
 * @author hutao
 * @date 2021/3/18 19:20
 * @description
 */
@Data
public class FlowBean {
    private String projectName;
    private String processDefinitionName;
    private String processDefinitionLocations;
    private String processDefinitionJson;
    private String processDefinitionConnects;
    private String serialization;
}
