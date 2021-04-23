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
import java.util.stream.Collectors;

/**
 * @author hutao
 * @date 2021/3/18 11:11
 * @description
 */
public class JobTransfer {

    public Map<String, Integer> nodeDepCountMap = new ConcurrentHashMap<>(2048);

    public Map<String, String> nodeContentMap = new HashMap<>(2048);

    public HashMultimap<String, String> nodeDepDetailMap = HashMultimap.create();

    public HashMultimap<String, String> nodeDepDetailMapReverse = HashMultimap.create();

    public List<String> jobs = new ArrayList<>(2048);

    public LinkedHashMultimap<String, Node> dags = LinkedHashMultimap.create();

    public LinkedHashMultimap<String, String> resourceMap = LinkedHashMultimap.create();

    public List<Pair<String, String>> list = new ArrayList<>();

    public Map<String, String> globalMap = new LinkedHashMap<>();

    public HashBasedTable<String, String, String> jobTables = HashBasedTable.create();

    public Map<String, String> flowMergersMapping = Maps.newHashMap();

    public Map<Integer, Integer> positionsMapping = Maps.newHashMap();

    public void clear() {
        nodeDepCountMap.clear();
        nodeContentMap.clear();
        nodeDepDetailMap.clear();
        jobs.clear();
        dags.clear();
        resourceMap.clear();
        list.clear();
        globalMap.clear();
        jobTables.clear();
        flowMergersMapping.clear();
    }

    public String trans(String path) throws Exception {
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
                List<String> contents = new ArrayList<>();
                Arrays.asList(StringUtils.split(str, "\n")).forEach(line -> {
                    if (StringUtils.startsWith(line.trim(), "command")) {
                        String m = StringUtils.replace(line, ":", "=");
                        String replace = StringUtils.replace(m, "command=", "command.999=");
                        contents.add(replace);
                    } else if (line.contains("dependencies=")) {
                        String[] deps = StringUtils.replace(line, "dependencies=", "").split(",");
                        Arrays.asList(deps).forEach(dep -> {
                            nodeDepDetailMap.put(id, dep.trim());
                            nodeDepDetailMapReverse.put(dep.trim(), id);
                            list.add(new Pair<>(id, dep.trim()));
                            nodeDepCountMap.computeIfPresent(dep.trim(), (k, v) -> v + 1);
                            nodeDepCountMap.computeIfAbsent(dep.trim(), k -> 1);
                        });
                    } else {
                        // 解析job文件中的附加属性
                        if (line.contains("=")) {
                            String[] split = line.split("=");
                            if (split.length < 2) {
                                System.out.println("skip param: " + line);
                            } else {
                                jobTables.put(id, split[0], split[1]);
                            }
                        } else {
                            System.out.println("error param ==> " + line);
                        }
                    }
                });
                if (!contents.isEmpty()) {
                    String command = contents.stream().map(c -> {
                        String[] cmd = c.split("=");
                        String index = StringUtils.split(cmd[0], ".")[1];
                        if (index.equals("999")) {
                            index = "-1";
                        }
                        return new Pair<>(Integer.parseInt(index), cmd[1]);
                    }).sorted(Comparator.comparingInt(Pair::getKey)).map(Pair::getValue).collect(Collectors.joining("\n"));
                    nodeContentMap.put(id, command);
                }

            }
        }));

        Maps.filterValues(nodeDepCountMap, v -> v == 0).keySet().forEach(k -> {
            System.out.println("job size====" + jobs.size());
            createDAG(Collections.singletonList(k), k, null, 0);
            System.out.println("dag: " + k + "  remaining job size:" + jobs.size());
        });

        flowMergersMapping.forEach((k, v) -> {
            Set<Node> nodes = dags.get(k);
            dags.putAll(v, new HashSet<>(nodes));
            dags.removeAll(k);
        });

        for (String s : dags.keySet()) {
            Set<Node> nodes = dags.get(s);
            List<String> startNodes = nodes.stream().filter(n -> n.getDeps().size() == 0).map(Node::getName).collect(Collectors.toList());
            Map<String, Node> collect = nodes.stream().collect(Collectors.toMap(Node::getName, n -> n, (v1, v2) -> v2));
            calcNodePosition(startNodes, collect, 1);
            dags.removeAll(s);
            List<Node> ns = new ArrayList<>(collect.values());
            optimizationDepth(ns);
            dags.putAll(s, ns);
        }

        System.out.println(dags);

        List<FlowBean> flowBeans = new ArrayList<>();
        for (String k : dags.keySet()) {
            FlowBean flow = createFlow(k, dags.get(k));
            flowBeans.add(flow);
        }
        return JSON.toJSONString(flowBeans);
    }

    public void optimizationDepth(Collection<Node> nodes){
        List<Integer> dps = nodes.stream().map(Node::getDepth).distinct().sorted().collect(Collectors.toList());
        Map<Integer, Integer> depthMapping = new HashMap<>();
        for (int i = 0; i < dps.size(); i++) {
            depthMapping.put(dps.get(i), i + 1);
        }
        nodes.forEach(n ->{
            n.setDepth(depthMapping.get(n.getDepth()));
        });
    }

    public void calcNodePosition(List<String> startNodes, Map<String, Node> allNodes, int depth) {
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < startNodes.size(); i++) {
            String name = startNodes.get(i);
            Node node = allNodes.get(name);
            node.setDepth(node.getDepth() + depth);
            node.setCurrDepthNodeCount(startNodes.size());
            if (node.getOrder() <= 1) {
                node.setOrder(calcOrderByDepth(node.getDepth(), node.getName(), allNodes));
            }
            node.getChildDeps().forEach( n -> {
                Node node1 = allNodes.get(n);
                node1.setDepth(node.getDepth() + 1);
            });
            nodes.addAll(node.getChildDeps());
        }
        if (!nodes.isEmpty()){
            calcNodePosition(nodes,allNodes,depth);
        }
    }

    public int calcOrderByDepth(int depth, String name, Map<String, Node> allNodes) {
        Integer order = positionsMapping.get(depth);
        if (order == null) {
            order = 1;
        }
        Node node = allNodes.get(name);
        Optional<Integer> min = node.getDeps().stream().map(n -> allNodes.get(n).getOrder()).min(Integer::compareTo);
        int parentMinOrder = min.orElse(1);
        order = Math.max(parentMinOrder, order);
        positionsMapping.put(depth, order + 1);
        return order;
    }

    public void createDAG(List<String> nodeNames, String flowName, Set<String> repeat, int depth) {
        List<String> nodesList = new ArrayList<>();
        if (repeat == null) {
            repeat = new HashSet<>();
        }
        List<Node> nodes = new ArrayList<>();
        for (String name : nodeNames) {
            if (jobs.contains(name)) {
                Node node = new Node();
                node.setName(name);
                node.setContent(nodeContentMap.getOrDefault(name, "echo '" + name + " success'"));
                ArrayList<String> list = new ArrayList<>(nodeDepDetailMap.get(name));
                ArrayList<String> listReverse = new ArrayList<>(nodeDepDetailMapReverse.get(name));
                node.setDeps(list);
                node.setChildDeps(listReverse);
                String fn = jobTables.get(name, "flowName");
                if (StringUtils.isNotBlank(fn)) {
                    node.setFlowName(fn);
                    if (!flowName.equals(fn)) {
                        flowMergersMapping.put(flowName, fn);
                        flowName = fn;
                    }
                } else {
                    node.setFlowName(flowName);
                    jobTables.put(name, "flowName", flowName);
                }
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
        for (Node node : nodes) {
            Set<String> set = dags.get(flowName).stream().map(Node::getName).collect(Collectors.toSet());
            if (!set.contains(node.getName())) {
                dags.put(flowName, node);
            }
        }
        if (!nodesList.isEmpty()) {
            createDAG(nodesList, flowName, repeat, depth + 3);
        }
    }

    public FlowBean createFlow(String flowEndTaskName, Collection<Node> nodes) {
        FlowBean flowBean = new FlowBean();
        flowBean.setProjectName("test");
        flowBean.setProcessDefinitionName(flowEndTaskName);
        flowBean.setProcessDefinitionConnects(createConnect(nodes));
        flowBean.setProcessDefinitionLocations(createLocation(nodes));
        flowBean.setProcessDefinitionJson(createTaskJson(nodes));
        return flowBean;
    }

    public String createConnect(Collection<Node> nodes) {
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

    public String createLocation(Collection<Node> nodes) {
        JSONObject json = new JSONObject(true);
        Optional<Node> max = nodes.stream().max(Comparator.comparingInt(Node::getDepth));
        nodes.forEach(node -> {
            LocationBean bean = new LocationBean();
            Pair<Integer, Integer> pair = coordinate(node.getDepth(), node.getOrder(), node.getCurrDepthNodeCount());
            bean.setName(node.getName());
            bean.setNodenumber(node.getDeps().size() + "");
            bean.setTargetarr(StringUtils.join(node.getDeps(), ","));
            bean.setX(pair.getKey());
            bean.setY(pair.getValue());
            json.put(bean.getName(), bean);
        });
        return json.toJSONString();
    }

    public Pair<Integer, Integer> coordinate(int depth, int count, int currDepthNodeCount) {
        int x = depth * 450 - 400;
        int y = count * 350 - 250;
        return new Pair<>(x, y);
    }

    public String createTaskJson(Collection<Node> nodes) {
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
            taskBean.setPhoneAlarmEnable(Boolean.parseBoolean(getValueByParamOrder("false", local.get("phone"), globalMap.get("phone"))));
            taskBean.setConditionResult(ConditionResultBean.builder().failedNode(new ArrayList<>()).successNode(new ArrayList<>()).build());
            taskBean.setDescription("");
            taskBean.setRunFlag("NORMAL");
            taskBean.setType("SHELL");
            taskBean.setTimeout(TimeoutBean.builder().enable(false).strategy("").build());
            taskBean.setMaxRetryTimes(getValueByParamOrder("1", local.get("retries"), globalMap.get("retries")));
            taskBean.setTaskInstancePriority(getValueByParamOrder("MEDIUM", local.get("priority"), globalMap.get("priority")).toUpperCase());
            taskBean.setDependence(new DependenceBean());
            taskBean.setRetryInterval(parseRetryInterval(getValueByParamOrder("1m", local.get("retry.backoff"), globalMap.get("retry.backoff"))) + "");
            taskBean.setMailAlarmEnable(Boolean.parseBoolean(getValueByParamOrder("true", local.get("mail"), globalMap.get("mail"))));
            taskBean.setPreTasks(node.getDeps());
            taskBean.setWorkerGroup("default");
            taskBean.setParams(ParamsBean.builder().rawScript(node.getContent()).localParams(new ArrayList<>()).resourceList(resourceBeans).build());
            taskList.add(taskBean);
        });
        definitionBean.setTasks(taskList);
        return JSON.toJSONString(definitionBean);
    }

    public String getValueByParamOrder(String defaultValue, String... params) {
        if (params == null || params.length == 0) {
            return defaultValue;
        }
        for (String paramName : params) {
            if (StringUtils.isNotBlank(paramName)) {
                return paramName;
            }
        }
        return defaultValue;
    }

    public long parseRetryInterval(String retryInterval) {
        String data = StringUtils.lowerCase(retryInterval);
        long min = 1;
        switch (data.replaceAll("\\d+", "").trim()) {
            case "h":
                min = Long.parseLong(data.replace("h", "")) * 60;
                break;
            case "m":
                min = Long.parseLong(data.replace("m", ""));
                break;
            case "s":
                min = Long.parseLong(data.replace("s", "")) / 60;
                break;
            default:
                min = Long.parseLong(data) / 60 / 1000;
                break;
        }
        return Math.max(min, 1);
    }

}
