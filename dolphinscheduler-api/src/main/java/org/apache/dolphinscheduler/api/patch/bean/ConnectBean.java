package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Data;

/**
 * @author hutao
 * @date 2021/3/18 19:37
 * @description
 */
@Data
public class ConnectBean {
    private String endPointSourceId;
    private String endPointTargetId;
}
