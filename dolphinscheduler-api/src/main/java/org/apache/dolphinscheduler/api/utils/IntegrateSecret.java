package org.apache.dolphinscheduler.api.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author hutao
 * @date 2021/1/21 17:19
 * @description
 */
@Slf4j
public class IntegrateSecret {

    private static final String SALT_KEY = "O49tDF4hy39Om3Qf";
    private static final String SEPARATOR = "!#!";
    private static final String UTF8 = "UTF-8";

    private static final String appId = "huking";
    private static final String appSecret = "hukingh2019yyme8u826ptmy1m21";

    public static String getDolphinSchedulerToken(String userCode,String email,String teamCode,String timestamp,String mobile){
        Map<String, String> paraMap = new HashMap<>();
        paraMap.put("user_domain_account", userCode);
        paraMap.put("email", email);
        paraMap.put("dept_en_name", teamCode);
        paraMap.put("ts", timestamp);
        paraMap.put("app_id", appId);
        paraMap.put("mobile", mobile);
        return creat3eHmaSha256(paraMap);
    }


    private static String creat3eHmaSha256(Map<String, String> paraMap) {
        final TreeMap<String, String> treeMap = new TreeMap<>();
        paraMap.forEach((k, v) -> {
            try {
                treeMap.put(k, URLEncoder.encode(v, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });

        final StringBuilder builder = new StringBuilder();
        treeMap.forEach((k, v) -> {
            if (!StringUtils.isBlank(k)) {
                if (0 < builder.length()) {
                    builder.append("&");
                }
                builder.append(k);
                if (!StringUtils.isBlank(v)) {
                    try {
                        builder.append("=").append(URLDecoder.decode(v, "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        log.debug("comp token-->param appSecret:{},builder:{}", appSecret, builder);
        try {
            // 签名算法HmacSha256
            Mac hmacSha256 = Mac.getInstance("HmacSHA256");
            byte[] userKeyBytes = appSecret.getBytes("UTF-8");
            hmacSha256.init(new SecretKeySpec(userKeyBytes, 0, userKeyBytes.length, "HmacSHA256"));
            byte[] userBytes = hmacSha256.doFinal(builder.toString().getBytes("UTF-8"));
            return Base64.getEncoder().encodeToString(userBytes);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }
}
