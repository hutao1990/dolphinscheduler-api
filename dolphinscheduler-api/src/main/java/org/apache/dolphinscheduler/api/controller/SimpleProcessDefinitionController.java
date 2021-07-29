package org.apache.dolphinscheduler.api.controller;

import io.swagger.annotations.*;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.SimpleProcessDefinitionService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.dao.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.HashMap;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.*;

/**
 * @author hutao
 * @date 2021/7/22 11:34
 * @description 简单工作流定义控制器
 */
@Api(tags = "SIMPLE_PROCESS_DEFINITION_TAG", position = 2)
@RestController
@RequestMapping("projects/{projectName}/simple/process")
public class SimpleProcessDefinitionController extends BaseController {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProcessDefinitionController.class);


    @Autowired
    private SimpleProcessDefinitionService simpleProcessDefinitionService;

    /**
     * query process definition list paging
     *
     * @param loginUser   login user
     * @param projectName project name
     * @param searchVal   search value
     * @param pageNo      page number
     * @param pageSize    page size
     * @param userId      user id
     * @return process definition page
     */
    @ApiOperation(value = "querySimpleProcessDefinitionListPaging", notes = "QUERY_PROCESS_DEFINITION_LIST_PAGING_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", required = true, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", required = false, type = "String"),
            @ApiImplicitParam(name = "userId", value = "USER_ID", required = false, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR)
    public Result queryProcessDefinitionListPaging(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                                   @RequestParam("pageNo") Integer pageNo,
                                                   @RequestParam(value = "searchVal", required = false) String searchVal,
                                                   @RequestParam(value = "userId", required = false, defaultValue = "0") Integer userId,
                                                   @RequestParam("pageSize") Integer pageSize) {
        logger.info("query process definition list paging, login user:{}, project name:{}", loginUser.getUserName(), projectName);
        Map<String, Object> result = checkPageParams(pageNo, pageSize);
        if (result.get(Constants.STATUS) != Status.SUCCESS) {
            return returnDataListPaging(result);
        }
        searchVal = ParameterUtils.handleEscapes(searchVal);
        result = simpleProcessDefinitionService.queryProcessDefinitionListPaging(loginUser, projectName, searchVal, pageNo, pageSize, userId);
        return returnDataListPaging(result);
    }

    /**
     * 创建简单工作量
     *
     * @param loginUser     登录用户
     * @param projectName   项目名称
     * @param name          工作流名称
     * @param cron          cron表达式
     * @param params        脚本参数
     * @param serialization 序列化标记
     * @param maxRetries    最大重试次数
     * @param mail          告警邮箱
     * @param phone         告警电话
     * @param timeout       超时时间
     * @return
     */
    @ApiOperation(value = "createSimpleProcessDefinition", notes = "CREATE_SIMPLE_PROCESS_DEFINITION")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "name", value = "NAME", required = true, dataType = "String", example = ""),
            @ApiImplicitParam(name = "cron", value = "CRON", required = true, dataType = "String", example = "0 0 1 * * ? *"),
            @ApiImplicitParam(name = "params", value = "PARAMS", required = false, dataType = "String", example = ""),
            @ApiImplicitParam(name = "serialization", value = "SERIALIZATION", required = false, dataType = "Int", example = "1"),
            @ApiImplicitParam(name = "maxRetries", value = "MAX_RETIES", required = false, dataType = "Int", example = "3"),
            @ApiImplicitParam(name = "mail", value = "EMAIL", required = true, dataType = "String", example = "test@gome.com.cn"),
            @ApiImplicitParam(name = "phone", value = "PHONE", required = false, dataType = "String", example = "13684737264"),
            @ApiImplicitParam(name = "timeout", value = "TIMEOUT", required = false, dataType = "Int", example = "5")
    })
    @PostMapping(value = "/save")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiException(CREATE_PROCESS_DEFINITION)
    public Result saveProcessDefinition(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                        @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                        @RequestParam(value = "name", required = true) String name,
                                        @RequestParam(value = "cron", required = true) String cron,
                                        @RequestParam(value = "params", required = false) String params,
                                        @RequestParam(value = "serialization", required = false) int serialization,
                                        @RequestParam(value = "maxRetries", required = false) int maxRetries,
                                        @RequestParam(value = "mail", required = true) String mail,
                                        @RequestParam(value = "phone", required = false) String phone,
                                        @RequestParam(value = "timeout", required = false) int timeout
    ) {
        Map<String, Object> result = simpleProcessDefinitionService.saveProcessDefine(loginUser, projectName, name, cron, params, serialization, maxRetries, mail, phone, timeout);
        return returnDataList(result);
    }


    /**
     * 更新简单工作流
     *
     * @param loginUser     登录用户
     * @param projectName   项目名称
     * @param id            工作流id
     * @param name          工作流名称
     * @param cron          cron表达式
     * @param params        脚本参数
     * @param serialization 系列化参数
     * @param maxRetries    最大重试次数
     * @param mail          告警邮箱
     * @param phone         告警电话
     * @param timeout       超时时间
     * @param status        工作流状态
     * @return
     */
    @ApiOperation(value = "updateSimpleProcessDefinition", notes = "UPDATE_SIMPLE_PROCESS_DEFINITION")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID", required = true, dataType = "Int", example = ""),
            @ApiImplicitParam(name = "name", value = "NAME", required = true, dataType = "String", example = ""),
            @ApiImplicitParam(name = "cron", value = "CRON", required = true, dataType = "String", example = "0 0 1 * * ? *"),
            @ApiImplicitParam(name = "params", value = "PARAMS", required = false, dataType = "String", example = ""),
            @ApiImplicitParam(name = "serialization", value = "SERIALIZATION", required = false, dataType = "Int", example = "1"),
            @ApiImplicitParam(name = "maxRetries", value = "MAX_RETIES", required = false, dataType = "Int", example = "3"),
            @ApiImplicitParam(name = "mail", value = "EMAIL", required = true, dataType = "String", example = "test@gome.com.cn"),
            @ApiImplicitParam(name = "phone", value = "PHONE", required = false, dataType = "String", example = "13684737264"),
            @ApiImplicitParam(name = "timeout", value = "TIMEOUT", required = false, dataType = "Int", example = "5"),
            @ApiImplicitParam(name = "status", value = "STATUS", required = false, dataType = "Int", example = "0")
    })
    @PostMapping(value = "/update")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(UPDATE_PROCESS_DEFINITION_ERROR)
    public Result updateProcessDefinition(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                          @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                          @RequestParam(value = "id", required = true) int id,
                                          @RequestParam(value = "name", required = true) String name,
                                          @RequestParam(value = "cron", required = true) String cron,
                                          @RequestParam(value = "params", required = false) String params,
                                          @RequestParam(value = "serialization", required = false) int serialization,
                                          @RequestParam(value = "maxRetries", required = false) int maxRetries,
                                          @RequestParam(value = "mail", required = true) String mail,
                                          @RequestParam(value = "phone", required = false) String phone,
                                          @RequestParam(value = "timeout", required = false) int timeout,
                                          @RequestParam(value = "status", required = true) int status
    ) {
        Map<String, Object> result = simpleProcessDefinitionService.updateProcessDefine(loginUser, projectName, id, name, cron, params, serialization, maxRetries, mail, phone, timeout, status);
        return returnDataList(result);
    }

    /**
     * 删除简单工作流
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return
     */
    @ApiOperation(value = "deleteSimpleProcessDefine", notes = "DELETE_SIMPLE_PROCESS_DEFINITION")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID", required = true, dataType = "Int", example = "")
    })
    @PostMapping(value = "/delete")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_PROCESS_DEFINE_BY_ID_ERROR)
    public Result deleteSimpleProcessDefine(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                            @RequestParam(value = "id", required = true) int id
    ) {
        Map<String, Object> result = simpleProcessDefinitionService.deleteById(loginUser, projectName, id);
        return returnDataList(result);
    }

    /**
     * 根据工作流id查询简单工作流
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return
     */
    @ApiOperation(value = "querySimpleProcessDefineById", notes = "QUERY_SIMPLE_PROCESS_DEFINE_BY_ID")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID", required = true, dataType = "Int", example = "")
    })
    @PostMapping(value = "/selectById")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_PROCESS_DEFINE_BY_ID_ERROR)
    public Result querySimpleProcessDefineById(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                               @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                               @RequestParam(value = "id", required = true) int id
    ) {
        Map<String, Object> result = simpleProcessDefinitionService.selectById(loginUser, projectName, id);
        return returnDataList(result);
    }

    /**
     * 简单工作流上线
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return
     */
    @ApiOperation(value = "simpleProcessDefineOnline", notes = "SIMPLE_PROCESS_DEFINE_ONLINE")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID", required = true, dataType = "Int", example = "")
    })
    @PostMapping(value = "/online")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_PROCESS_DEFINE_BY_ID_ERROR)
    public Result simpleProcessDefineOnline(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                            @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                            @RequestParam(value = "id", required = true) int id
    ) {
        Map<String, Object> result = simpleProcessDefinitionService.online(loginUser, projectName, id);
        return returnDataList(result);
    }

    /**
     * 简单工作流下线
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return
     */
    @ApiOperation(value = "simpleProcessDefineOffline", notes = "SIMPLE_PROCESS_DEFINE_OFFLINE")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID", required = true, dataType = "Int", example = "")
    })
    @PostMapping(value = "/offline")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_PROCESS_DEFINE_BY_ID_ERROR)
    public Result simpleProcessDefineOffline(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                             @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                             @RequestParam(value = "id", required = true) int id
    ) {
        Map<String, Object> result = simpleProcessDefinitionService.offline(loginUser, projectName, id);
        return returnDataList(result);
    }

    /**
     * 简单工作流下线
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @param id          工作流id
     * @return
     */
    @ApiOperation(value = "editSimpleProcessDefineContent", notes = "EDIT_SIMPLE_PROCESS_DEFINE_CONTENT")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID", required = true, dataType = "Int", example = ""),
            @ApiImplicitParam(name = "content", value = "CONTENT", required = true, dataType = "String", example = "")
    })
    @PostMapping(value = "/editContent")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_PROCESS_DEFINE_BY_ID_ERROR)
    public Result editContent(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                              @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                              @RequestParam(value = "id", required = true) int id,
                              @RequestParam(value = "content", required = true) String content
    ) {
        Map<String, Object> result = simpleProcessDefinitionService.updateContentById(loginUser, projectName, id, content);
        return returnDataList(result);
    }



}
