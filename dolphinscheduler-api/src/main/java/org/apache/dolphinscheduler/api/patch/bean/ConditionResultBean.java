package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author hutao
 * @date 2021/3/19 11:25
 * @description
 */
@Data
@Builder
public class ConditionResultBean {
    private List<String> successNode;
    private List<String> failedNode;
}
