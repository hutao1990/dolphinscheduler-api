package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author hutao
 * @date 2021/3/22 14:29
 * @description
 */
@Data
@EqualsAndHashCode
public class Node {
    private int depth;
    private int order;
    private String name;
    private String content;
    private List<String> deps;
    private int childNum;
    private String flowName;
}
