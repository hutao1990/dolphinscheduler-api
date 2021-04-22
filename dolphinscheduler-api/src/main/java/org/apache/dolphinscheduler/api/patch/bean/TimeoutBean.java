package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Builder;
import lombok.Data;

/**
 * @author hutao
 * @date 2021/3/19 11:27
 * @description
 */
@Data
@Builder
public class TimeoutBean {
    private boolean enable;
    private String strategy;
}
