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

import static org.apache.dolphinscheduler.api.enums.Status.CREATE_PROCESS_DEFINITION;
import static org.apache.dolphinscheduler.api.enums.Status.QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR;

/**
 * @author hutao
 * @date 2021/7/22 11:34
 * @description 简单工作流定义控制器
 */
@Api(tags = "SIMPLE_PROCESS_DEFINITION_TAG", position = 2)
@RestController
@RequestMapping("projects/{projectName}/simple/process")
public class SimpleProcessDefinitionController extends BaseController{

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
    @ApiOperation(value = "querySimpleProcessDefinitionListPaging", notes= "QUERY_PROCESS_DEFINITION_LIST_PAGING_NOTES")
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

    @ApiOperation(value = "createSimpleProcessDefinition", notes= "CREATE_SIMPLE_PROCESS_DEFINITION")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "name",value = "NAME",required = true,dataType = "String",example = ""),
            @ApiImplicitParam(name = "cron",value = "CRON",required = true,dataType = "String",example = "0 0 1 * * ? *"),
            @ApiImplicitParam(name = "params",value = "PARAMS",required = false,dataType = "String",example = ""),
            @ApiImplicitParam(name = "serialization",value = "SERIALIZATION",required = false,dataType = "Int",example = "1"),
            @ApiImplicitParam(name = "maxRetries",value = "MAX_RETIES",required = false,dataType = "Int",example = "3"),
            @ApiImplicitParam(name = "mail",value = "EMAIL",required = true,dataType = "String",example = "test@gome.com.cn"),
            @ApiImplicitParam(name = "phone",value = "PHONE",required = false,dataType = "String",example = "13684737264"),
            @ApiImplicitParam(name = "timeout",value = "TIMEOUT",required = false,dataType = "Int",example = "5")
    })
    @PostMapping(value = "/save")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiException(CREATE_PROCESS_DEFINITION)
    public Result saveProcessDefinition(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                        @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                        @RequestParam(value = "name", required = true) String name,
                                        @RequestParam(value = "cron",required = true) String cron,
                                        @RequestParam(value = "params", required = false) String params,
                                        @RequestParam(value = "serialization", required = false) int serialization,
                                        @RequestParam(value = "maxRetries", required = false) int maxRetries,
                                        @RequestParam(value = "mail", required = true) String mail,
                                        @RequestParam(value = "phone", required = false) String phone,
                                        @RequestParam(value = "timeout", required = false) int timeout
                                        ){
        Map<String, Object> result = simpleProcessDefinitionService.saveProcessDefine(loginUser,projectName,name,cron,params,serialization,maxRetries,mail,phone,timeout);
        return returnDataList(result);
    }

}
