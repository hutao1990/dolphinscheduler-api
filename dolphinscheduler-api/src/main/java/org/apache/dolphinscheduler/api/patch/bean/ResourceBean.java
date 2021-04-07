package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Builder;
import lombok.Data;

/**
 * @author hutao
 * @date 2021/3/19 14:00
 * @description
 */
@Data
@Builder
public class ResourceBean {
    private String res;
    private String name;
    private int id;
}
