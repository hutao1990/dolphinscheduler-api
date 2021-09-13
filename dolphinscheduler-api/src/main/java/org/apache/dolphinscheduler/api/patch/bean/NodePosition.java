package org.apache.dolphinscheduler.api.patch.bean;

import lombok.Data;

import java.util.*;

/**
 * @author hutao
 * @date 2021/9/9 17:23
 * @description
 */
@Data
public class NodePosition {
    private String name;
    private int depth;
    private int order;
    private int width;
    private Set<String> preDeps = new TreeSet<>();
    private Map<Integer, List<String>> preDepsMap = new HashMap<>();
    private Map<Integer, Integer> preDepsIndexMap = new HashMap<>();
    private Set<String> afterDeps = new TreeSet<>();
    private int length;


    public int getLength() {
        return name.length();
    }

    public boolean isFirst() {
        return preDeps == null || preDeps.size() == 0;
    }

    public boolean isEnd() {
        return afterDeps == null || afterDeps.size() == 0;
    }

}
