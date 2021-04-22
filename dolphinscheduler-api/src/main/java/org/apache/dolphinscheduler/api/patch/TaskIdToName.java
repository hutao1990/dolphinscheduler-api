package org.apache.dolphinscheduler.api.patch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hutao
 * @date 2021/4/16 18:38
 * @description
 */
public class TaskIdToName {

    private List<String> ids = new ArrayList<>();
    private List<String> names = new ArrayList<>();

    public TaskIdToName(String text){
        JSONObject obj = JSON.parseObject(text);
        JSONArray tasks = obj.getJSONArray("tasks");
        for (int i = 0; i < tasks.size(); i++) {
            JSONObject task = tasks.getJSONObject(i);
            String id = task.getString("id");
            String name = task.getString("name");
            if (!id.equals(name)){
                ids.add(id);
                names.add(name);
            }
        }

    }

    public String transfer(String json){
        return StringUtils.replaceEach(json,ids.toArray(new String[]{}),names.toArray(new String[]{}));
    }

}
