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
package org.apache.dolphinscheduler.api.security;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.SessionService;
import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.api.utils.IntegrateSecret;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.Session;
import org.apache.dolphinscheduler.dao.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.http.HttpServletRequest;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Map;

public class PasswordAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(PasswordAuthenticator.class);

    @Autowired
    private UsersService userService;
    @Autowired
    private SessionService sessionService;

    @Override
    public Result<Map<String, String>> authenticate(String username, String password, String extra) {
        Result<Map<String, String>> result = new Result<>();
        // verify username and password
        User user = userService.queryUser(username, password);
        if (user == null) {
            result.setCode(Status.USER_NAME_PASSWD_ERROR.getCode());
            result.setMsg(Status.USER_NAME_PASSWD_ERROR.getMsg());
            return result;
        }

        // create session
        String sessionId = sessionService.createSession(user, extra);
        if (sessionId == null) {
            result.setCode(Status.LOGIN_SESSION_FAILED.getCode());
            result.setMsg(Status.LOGIN_SESSION_FAILED.getMsg());
            return result;
        }
        logger.info("sessionId : {}", sessionId);
        result.setData(Collections.singletonMap(Constants.SESSION_ID, sessionId));
        result.setCode(Status.SUCCESS.getCode());
        result.setMsg(Status.LOGIN_SUCCESS.getMsg());
        return result;
    }

    /**
     * put message to result object
     *
     * @param result       result code
     * @param status       status
     * @param statusParams status message
     */
    protected void putMsg(Result result, Status status, Object... statusParams) {
        result.setCode(status.getCode());

        if (statusParams != null && statusParams.length > 0) {
            result.setMsg(MessageFormat.format(status.getMsg(), statusParams));
        } else {
            result.setMsg(status.getMsg());
        }

    }


    @Override
    public Result<Map<String, String>> authenticateTokens(String userCode, String email, String teamCode, String timestamp, String token, String extra,String mobile) {
        Result<Map<String, String>> result = new Result<>();

        if ("admin".equals(userCode.toLowerCase())) {
            result.setCode(Status.ADMIN_LOGIN_AS_TOKEN_ERROR.getCode());
            putMsg(result, Status.ADMIN_LOGIN_AS_TOKEN_ERROR);
            return result;
        }

        long curr = System.currentTimeMillis();
        long tokenTime = Long.parseLong(timestamp);

        long minute = (curr - tokenTime) / 1000 / 60;
        if (minute > 10) {
            result.setCode(Status.TOKEN_TIME_OUT.getCode());
            putMsg(result, Status.TOKEN_TIME_OUT);
            return result;
        }


        // verify username
        User user = userService.queryUser(userCode);
        if (user == null) {
            try {
                Map<String, Object> map = userService.createUser(userCode, userCode + ".123" + RandomUtils.nextInt(100, 1000), email, teamCode, mobile);
                if (StringUtils.equalsAny(map.get("msg").toString(),"success","成功")) {
                    user = userService.queryUser(userCode);
                }else {
                    throw new Exception("create user error！");
                }
            } catch (Exception e) {
                e.printStackTrace();
                result.setCode(Status.CREATE_USER_ERROR.getCode());
                result.setMsg(Status.CREATE_USER_ERROR.getMsg());
                return result;
            }
        }


        String tokenTmp = IntegrateSecret.getDolphinSchedulerToken(userCode, email, teamCode, timestamp,mobile);
        if (!token.equals(tokenTmp)) {
            result.setCode(Status.AUTHORIZED_USER_ERROR.getCode());
            putMsg(result, Status.AUTHORIZED_USER_ERROR, userCode);
            return result;
        }


        // create session
        String sessionId = sessionService.createSession(user, extra);
        if (sessionId == null) {
            result.setCode(Status.LOGIN_SESSION_FAILED.getCode());
            result.setMsg(Status.LOGIN_SESSION_FAILED.getMsg());
            return result;
        }
        logger.info("sessionId : {}", sessionId);
        result.setData(Collections.singletonMap(Constants.SESSION_ID, sessionId));
        result.setCode(Status.SUCCESS.getCode());
        result.setMsg(Status.LOGIN_SUCCESS.getMsg());
        return result;
    }

    @Override
    public User getAuthUser(HttpServletRequest request) {
        Session session = sessionService.getSession(request);
        if (session == null) {
            logger.info("session info is null ");
            return null;
        }
        //get user object from session
        return userService.queryUser(session.getUserId());
    }
}
