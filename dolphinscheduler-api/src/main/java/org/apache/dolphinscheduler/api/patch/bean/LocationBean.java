package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Data;

/**
 * @author hutao
 * @date 2021/3/18 19:33
 * @description
 */
@Data
public class LocationBean {
    private String name;
    private String targetarr;
    private String nodenumber;
    private int x;
    private int y;
}
