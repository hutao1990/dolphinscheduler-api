package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Builder;
import lombok.Data;

/**
 * @author hutao
 * @date 2021/3/18 19:44
 * @description
 */
@Data
@Builder
public class GlobalParamsBean {
    private String prop;
    private String direct;
    private String type;
    private String value;
}
