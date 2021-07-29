/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.api.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.dolphinscheduler.api.security.Authenticator;
import org.apache.dolphinscheduler.api.service.SessionService;
import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Map;

/**
 * login interceptor, must login first
 */
public class LoginHandlerInterceptor implements HandlerInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(LoginHandlerInterceptor.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private Authenticator authenticator;

    @Autowired
    private UsersService userService;

    private static Decoder decoder = Base64.getDecoder();

    /**
     * Intercept the execution of a handler. Called after HandlerMapping determined
     *
     * @param request  current HTTP request
     * @param response current HTTP response
     * @param handler  chosen handler to execute, for type and/or instance evaluation
     * @return boolean true or false
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {

        String requestURI = request.getRequestURI();
        if (requestURI.contains("swagger-ui.html")){
            return true;
        }
        // get token
        String token = request.getHeader("token");
        User user = null;
        if (StringUtils.isEmpty(token)) {
            user = authenticator.getAuthUser(request);
            //
            if (user == null) {
                String usercode = request.getHeader("usercode");
                String teamcode = request.getHeader("teamcode");
                String accesstoken = request.getHeader("accesstoken");
                if (accesstoken == null){
                    logger.error("huking login token is null!");
                    return false;
                }
                String[] split = accesstoken.split("\\.");
                if (split.length != 3) {
                    return false;
                }
                JSONObject json = JSON.parseObject(new String(decoder.decode(split[1])));
                if (System.currentTimeMillis() / 1000 > json.getLong("exp")) {
                    logger.error("accesstoken is expired！");
                    return false;
                }

                user = userService.queryUser(teamcode);
                if (user == null) {
                    try {
                        Map<String, Object> map = userService.createUser(teamcode, teamcode + ".123" + RandomUtils.nextInt(100, 1000), usercode + "@gome.com.cn", teamcode, null);
                        if (org.apache.commons.lang3.StringUtils.equalsAny(map.get("msg").toString(), "success", "成功")) {
                            user = userService.queryUser(teamcode);
                        } else {
                            throw new Exception("create user error！");
                        }
                    } catch (Exception e) {
                        response.setStatus(HttpStatus.SC_UNAUTHORIZED);
                        logger.info("user create failed!");
                        return false;
                    }
                }
            }
            // if user is null
            if (user == null) {
                response.setStatus(HttpStatus.SC_UNAUTHORIZED);
                logger.info("user does not exist");
                return false;
            }


        } else {
            user = userMapper.queryUserByToken(token);
            if (user == null) {
                response.setStatus(HttpStatus.SC_UNAUTHORIZED);
                logger.info("user token has expired");
                return false;
            }
        }
        request.setAttribute(Constants.SESSION_USER, user);
        return true;
    }
}
