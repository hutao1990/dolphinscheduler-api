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
    private int preLineCount;
    private int minLineCount;
    private List<String> adjoin = new ArrayList<>();
    private Set<String> preDeps = new TreeSet<>();
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

    public int getTotalLineCount(){
        return this.preLineCount + (this.getPreDeps() == null? 0: this.getPreDeps().size());
    }

}
