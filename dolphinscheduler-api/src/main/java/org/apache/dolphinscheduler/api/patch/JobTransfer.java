package org.apache.dolphinscheduler.api.patch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.*;
import javafx.util.Pair;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.patch.bean.*;
import org.apache.dolphinscheduler.api.patch.utils.ZipUtils;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hutao
 * @date 2021/3/18 11:11
 * @description
 */
public class JobTransfer {

    public static Map<String, Integer> nodeDepCountMap = new ConcurrentHashMap<>(2048);

    public static Map<String, String> nodeContentMap = new HashMap<>(2048);

    public static HashMultimap<String, String> nodeDepDetailMap = HashMultimap.create();

    public static List<String> jobs = new ArrayList<>(2048);

    public static LinkedHashMultimap<String, Node> dags = LinkedHashMultimap.create();

    public static LinkedHashMultimap<String, String> resourceMap = LinkedHashMultimap.create();

    public static List<Pair<String, String>> list = new ArrayList<>();

    public static Map<String, String> globalMap = new LinkedHashMap<>();

    public static HashBasedTable<String, String,String> jobTables = HashBasedTable.create();


    public static void clear(){
        nodeDepCountMap.clear();
        nodeContentMap.clear();
        nodeDepDetailMap.clear();
        jobs.clear();
        dags.clear();
        resourceMap.clear();
        list.clear();
        globalMap.clear();
        jobTables.clear();
    }

    public static String trans(String path) throws Exception {
        clear();
        ZipUtils.readZipFile(path, Arrays.asList(".job", ".properties"), ((name, fileName, content) ->
        {
            if (fileName.endsWith(".properties")) {
                Arrays.asList(StringUtils.split(content, "\n")).forEach(line -> {
                    if (line.contains("=") && !line.startsWith("#")) {
                        String[] split = line.split("=");
                        if (split.length > 1) {
                            globalMap.put(split[0], split[1]);
                        }
                    } else {
                        System.out.println(name + " --> error param: " + line);
                    }
                });
            } else {
                String id = StringUtils.split(fileName, ".")[0].trim();
                nodeDepCountMap.computeIfAbsent(id, k -> 0);
                jobs.add(id);
                String str = content.replaceAll("\\\\[ ]{0,5}\n", "");
                Arrays.asList(StringUtils.split(str, "\n")).forEach(line -> {
                    if (StringUtils.containsAny(line, "command=", "command:")) {
                        nodeContentMap.put(id, StringUtils.replaceEach(line, new String[]{"command=", "command:"}, new String[]{"", ""}));
                    } else if (line.contains("dependencies=")) {
                        String[] deps = StringUtils.replace(line, "dependencies=", "").split(",");
                        Arrays.asList(deps).forEach(dep -> {
                            nodeDepDetailMap.put(id, dep.trim());
                            list.add(new Pair<>(id, dep.trim()));
                            nodeDepCountMap.computeIfPresent(dep.trim(), (k, v) -> v + 1);
                            nodeDepCountMap.computeIfAbsent(dep.trim(), k -> 1);
                        });
                    }else {
                        // 解析job文件中的附加属性
                        if (line.contains("=")) {
                            String[] split = line.split("=");
                            if (split.length < 2){
                                System.out.println("skip param: "+line);
                            }else {
                                jobTables.put(id, split[0],split[1]);
                            }
                        }else {
                            System.out.println("error param ==> "+line);
                        }
                    }
                });
            }
        }));

        Maps.filterValues(nodeDepCountMap, v -> v == 0).keySet().forEach(k -> {
            System.out.println("job size====" + jobs.size());
            createDAG(Collections.singletonList(k), k, null, 0);
            System.out.println("dag: " + k + "  remaining job size:" + jobs.size());
        });
        System.out.println(dags);

        List<FlowBean> flowBeans = new ArrayList<>();
        for (String k : dags.keySet()) {
            FlowBean flow = createFlow(k, dags.get(k));
            flowBeans.add(flow);
        }
        return JSON.toJSONString(flowBeans);
    }

    public static void createDAG(List<String> nodeNames, String flowName, Set<String> repeat, int depth) {
        List<String> nodesList = new ArrayList<>();
        if (repeat == null) {
            repeat = new HashSet<>();
        }
        List<Node> nodes = new ArrayList<>();
        for (String name : nodeNames) {
            if (jobs.contains(name)) {
                Node node = new Node();
                node.setName(name);
                node.setDepth(depth);
                node.setChildNum(nodeDepCountMap.get(name));
                node.setContent(nodeContentMap.getOrDefault(name, "echo '" + name + " success'"));
                ArrayList<String> list = new ArrayList<>(nodeDepDetailMap.get(name));
                node.setDeps(list);
                for (String s : list) {
                    if (!repeat.contains(s)) {
                        repeat.add(s);
                        nodesList.add(s);
                    }
                }
                nodes.add(node);
//                jobs.remove(name);
            } else {
                System.out.println("job name not in jobList,please check job dependencies!");
                throw new IllegalStateException("job name not in jobList,please check job dependencies!");
            }
        }
        nodes.sort(Comparator.comparingInt(n -> -(n.getDeps().size() + n.getChildNum())));
        Collection<String> transform = Collections2.transform(nodes, Node::getName);
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            node.setOrder(i);
            Collection<String> intersection = CollectionUtils.intersection(node.getDeps(), transform);
            if (!CollectionUtils.isEmpty(intersection)) {
                node.setDepth(node.getDepth() - 1);
            }
            if (node.getDeps().size() > 25) {
                node.setDepth(node.getDepth() - 2);
            }
            dags.put(flowName, node);
        }
        if (!nodesList.isEmpty()) {
            createDAG(nodesList, flowName, repeat, depth + 3);
        }
    }

    public static FlowBean createFlow(String flowEndTaskName, Collection<Node> nodes) {
        FlowBean flowBean = new FlowBean();
        flowBean.setProjectName("test");
        flowBean.setProcessDefinitionName(flowEndTaskName);
        flowBean.setProcessDefinitionConnects(createConnect(nodes));
        flowBean.setProcessDefinitionLocations(createLocation(nodes));
        flowBean.setProcessDefinitionJson(createTaskJson(nodes));
        return flowBean;
    }

    public static String createConnect(Collection<Node> nodes) {
        List<ConnectBean> connects = new ArrayList<>();
        for (Node node : nodes) {
            node.getDeps().forEach(dep -> {
                ConnectBean bean = new ConnectBean();
                bean.setEndPointSourceId(dep);
                bean.setEndPointTargetId(node.getName());
                connects.add(bean);
            });
        }
        return JSON.toJSONString(connects);
    }

    public static String createLocation(Collection<Node> nodes) {
        JSONObject json = new JSONObject(true);
        Optional<Node> max = nodes.stream().max(Comparator.comparingInt(Node::getDepth));
        int maxDepth = max.get().getDepth();
        nodes.forEach(node -> {
            LocationBean bean = new LocationBean();
            Pair<Integer, Integer> pair = coordinate(maxDepth, node.getDepth(), node.getOrder());
            bean.setName(node.getName());
            bean.setNodenumber(node.getDeps().size() + "");
            bean.setTargetarr(StringUtils.join(node.getDeps(), ","));
            bean.setX(pair.getKey());
            bean.setY(pair.getValue());
            json.put(bean.getName(), bean);
        });
        return json.toJSONString();
    }

    public static Pair<Integer, Integer> coordinate(int maxDepth, int depth, int count) {
        int x = maxDepth - depth;
        int y = count;
        return new Pair<>(y * 250 + 50, x * 500 + 100);
    }

    public static String createTaskJson(Collection<Node> nodes) {
        DefinitionBean definitionBean = new DefinitionBean();
        List<GlobalParamsBean> params = new ArrayList<>();
        globalMap.forEach((k, v) -> {
            GlobalParamsBean build = GlobalParamsBean.builder()
                    .prop(k.trim())
                    .direct("IN")
                    .type("VARCHAR")
                    .value(v.trim())
                    .build();
            params.add(build);
        });
        definitionBean.setGlobalParams(params);
        definitionBean.setTenantId(0);
        definitionBean.setTimeout(0);
        List<TaskBean> taskList = new ArrayList<>();
        nodes.forEach(node -> {
            TaskBean taskBean = new TaskBean();
            Set<String> res = resourceMap.get(node.getName());
            List<ResourceBean> resourceBeans = new ArrayList<>();
            res.forEach(s -> {
                resourceBeans.add(ResourceBean.builder().name(s).build());
            });
            Map<String, String> local = jobTables.row(node.getName());
            taskBean.setId(node.getName());
            taskBean.setName(node.getName());
            taskBean.setPhoneAlarmEnable(Boolean.parseBoolean(getValueByParamOrder("false",local.get("phone"),globalMap.get("phone"))));
            taskBean.setConditionResult(ConditionResultBean.builder().failedNode(new ArrayList<>()).successNode(new ArrayList<>()).build());
            taskBean.setDescription("");
            taskBean.setRunFlag("NORMAL");
            taskBean.setType("SHELL");
            taskBean.setTimeout(TimeoutBean.builder().enable(false).strategy("").build());
            taskBean.setMaxRetryTimes(getValueByParamOrder("1", local.get("retries"),globalMap.get("retries")));
            taskBean.setTaskInstancePriority(getValueByParamOrder("MEDIUM",local.get("priority"),globalMap.get("priority")).toUpperCase());
            taskBean.setDependence(new DependenceBean());
            taskBean.setRetryInterval(parseRetryInterval(getValueByParamOrder("1m",local.get("retry.backoff"),globalMap.get("retry.backoff")))+"");
            taskBean.setMailAlarmEnable(Boolean.parseBoolean(getValueByParamOrder("true",local.get("mail"),globalMap.get("mail"))));
            taskBean.setPreTasks(node.getDeps());
            taskBean.setWorkerGroup("default");
            taskBean.setParams(ParamsBean.builder().rawScript(node.getContent()).localParams(new ArrayList<>()).resourceList(resourceBeans).build());
            taskList.add(taskBean);
        });
        definitionBean.setTasks(taskList);
        return JSON.toJSONString(definitionBean);
    }

    public static String getValueByParamOrder(String defaultValue, String... params){
        if (params == null || params.length == 0){
            return defaultValue;
        }
        for (String paramName : params) {
            if (StringUtils.isNotBlank(paramName)){
                return paramName;
            }
        }
        return defaultValue;
    }

    public static long parseRetryInterval(String retryInterval){
        String data = StringUtils.lowerCase(retryInterval);
        long min = 1;
        switch (data.replaceAll("\\d+]","").trim()){
            case "h":
                min = Long.parseLong(data.replace("h","")) * 60;
                break;
            case "m":
                min = Long.parseLong(data.replace("m",""));
                break;
            case "s":
                min = Long.parseLong(data.replace("s","")) / 60;
                break;
            default :
                min = Long.parseLong(data) / 60 / 1000;
                break;
        }
        return Math.max(min,1);
    }

}
