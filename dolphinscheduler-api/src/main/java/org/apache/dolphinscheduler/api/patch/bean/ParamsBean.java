package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author hutao
 * @date 2021/3/19 11:27
 * @description
 */
@Data
@Builder
public class ParamsBean {
    private String rawScript;
    private List<GlobalParamsBean> localParams;
    private List<ResourceBean> resourceList;
}
