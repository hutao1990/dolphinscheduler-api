package org.apache.dolphinscheduler.api.patch;

import lombok.Cleanup;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.IOUtils;
import org.apache.dolphinscheduler.api.patch.utils.ZipUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author hutao
 * @date 2021/4/7 17:27
 * @description
 */
public class AzJobTransferUtil {

    public static String azJob2JsonString(MultipartFile file) throws Exception {
        String name = file.getOriginalFilename();
        String fileName = name.substring(name.lastIndexOf("/") + 1);
        String[] split = fileName.split("\\.");
        String path1 = "/tmp/"+split[0]+"-tmp1."+split[1];
        String path2 = "/tmp/"+split[0]+"-tmp2."+split[1];
        @Cleanup FileOutputStream out = new FileOutputStream(new File(path1));
        IOUtils.copy(file.getInputStream(),out);
        ZipUtils.transform(path1,path2);
        return JobTransfer.trans(path2);
    }
}
