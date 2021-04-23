package org.apache.dolphinscheduler.api.patch.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import lombok.Cleanup;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.patch.inter.StreamOperate;
import org.apache.dolphinscheduler.api.patch.inter.StringOperate;
import org.apache.dolphinscheduler.api.patch.inter.ZipReadOperate;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.List;

/**
 * @author hutao
 * @date 2021/3/18 14:40
 * @description
 */
public class ZipUtils {

    public static void readZipFile(String path, List<String> suffixs, ZipReadOperate opt) throws IOException {

        ZipFile zipFile = new ZipFile(path);
        Enumeration<ZipArchiveEntry> entries = zipFile.getEntriesInPhysicalOrder();
        while (entries.hasMoreElements()) {
            ZipArchiveEntry entry = entries.nextElement();
            String name = entry.getName();
//            System.out.println(name);
            String content = IOUtils.toString(zipFile.getInputStream(entry));
            if (entry.isDirectory()) {
                continue;
            }
            if (StringUtils.endsWithAny(name, suffixs.toArray(new String[]{}))) {
                opt.operate(name, name.substring(name.lastIndexOf("/") + 1), StringUtils.replaceChars(content,"\r",""));
            }
        }
    }

    public static void zipTransfer(String path, String ouptputPath, List<String> suffixs, StreamOperate opt) throws IOException {

        ZipFile zipFile = new ZipFile(path);
        String fileName = path.substring(path.lastIndexOf(File.separatorChar) + 1);
        String fn = StringUtils.split(fileName, ".")[0];
        Enumeration<ZipArchiveEntry> entries = zipFile.getEntriesInPhysicalOrder();
        @Cleanup ZipArchiveOutputStream out = new ZipArchiveOutputStream(new File(ouptputPath + "/output-" + fn + ".zip"));
        out.setUseZip64(Zip64Mode.AsNeeded);
        String[] sf = suffixs.toArray(new String[]{});
        while (entries.hasMoreElements()) {
            ZipArchiveEntry entry = entries.nextElement();
            String name = entry.getName();
            out.putArchiveEntry(entry);
            if (StringUtils.endsWithAny(name, sf)) {
                opt.operate(name, zipFile.getInputStream(entry), out);
            } else {
                IOUtils.copy(zipFile.getInputStream(entry), out);
            }
            out.closeArchiveEntry();
        }
        out.finish();
    }

    public static void zipTransferString(String path, String ouptputPath, List<String> suffixs, StringOperate opt) throws IOException {
        zipTransfer(path, ouptputPath, suffixs, (name, in, out) -> {
            String content = IOUtils.toString(in);
            String handle = opt.handle(name, content);
            IOUtils.copy(IOUtils.toInputStream(handle), out);
        });
    }

    public static void transform(String path, String ouptputPath) throws Exception {
        ZipFile zipFile = new ZipFile(path);
        Enumeration<ZipArchiveEntry> entries = zipFile.getEntriesInPhysicalOrder();
        @Cleanup ZipArchiveOutputStream out = new ZipArchiveOutputStream(new File(ouptputPath));
        out.setUseZip64(Zip64Mode.AsNeeded);
        while (entries.hasMoreElements()) {
            ZipArchiveEntry entry = entries.nextElement();
            String name = entry.getName();
            if (name.endsWith(".flow")) {
                String fileName = name.substring(name.lastIndexOf("/") + 1);
                String flowName = StringUtils.split(fileName,".")[0];
                @Cleanup InputStream in = zipFile.getInputStream(entry);
                Yaml yaml = new Yaml();
                Object load = yaml.load(in);
                JSONObject json = JSON.parseObject(JSON.toJSONString(load));
//                System.out.println(JSON.toJSONString(json, true));
                String config = Joiner.on("\n").withKeyValueSeparator("=").join(json.getJSONObject("config"));
                ZipArchiveEntry archiveEntry = new ZipArchiveEntry("config.properties");
                out.putArchiveEntry(archiveEntry);
                out.write(config.getBytes(StandardCharsets.UTF_8));
                out.closeArchiveEntry();
                JSONArray nodes = json.getJSONArray("nodes");
                for (int i = 0; i < nodes.size(); i++) {
                    JSONObject node = nodes.getJSONObject(i);
                    JSONArray nodes1 = node.getJSONArray("nodes");
                    if (nodes1!= null && !nodes1.isEmpty()){
                        throw new IllegalArgumentException("不支持azkaban子流程格式！");
                    }
                    StringBuilder sb = new StringBuilder();
                    String type = node.getString("type");
                    sb.append("flowName=").append(flowName).append("\n");
                    sb.append("type=").append(type).append("\n");
                    JSONObject cfg = node.getJSONObject("config");
                    if (cfg != null) {
                        sb.append(Joiner.on("\n").withKeyValueSeparator("=").join(cfg)).append("\n");
                    }
                    JSONArray dependsOn = node.getJSONArray("dependsOn");
                    if (dependsOn!= null && dependsOn.size() > 0) {
                        sb.append("dependencies=").append(StringUtils.join(dependsOn, ","));
                    }
                    ZipArchiveEntry nodeEntry = new ZipArchiveEntry(node.getString("name")+".job");
                    out.putArchiveEntry(nodeEntry);
                    out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                    out.closeArchiveEntry();
                }
            } else {
                out.putArchiveEntry(entry);
                IOUtils.copy(zipFile.getInputStream(entry), out);
                out.closeArchiveEntry();
            }
        }

    }
}
