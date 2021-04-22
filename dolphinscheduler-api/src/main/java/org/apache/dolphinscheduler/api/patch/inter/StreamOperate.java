package org.apache.dolphinscheduler.api.patch.inter;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author hutao
 * @date 2021/3/26 15:10
 * @description
 */
public interface StreamOperate {
    void operate(String name,InputStream in, OutputStream out) throws IOException;
}
