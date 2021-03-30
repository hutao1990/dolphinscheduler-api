package org.apache.dolphinscheduler.api.utils;

import org.springframework.util.FileCopyUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;

/**
 * @author hutao
 * @date 2021/3/22 16:23
 * @description
 */
public class ZipMultiPartFile implements MultipartFile, Serializable {

    private String name;
    private String originalFilename;
    private String contentType;
    private byte[] data;
    private String fullName;

    public ZipMultiPartFile(String originalFilename,byte[] data) {
        this.originalFilename = originalFilename.substring(originalFilename.lastIndexOf("/") + 1);
        this.name = originalFilename.substring(originalFilename.lastIndexOf("/") + 1);
        this.contentType = name.substring(name.lastIndexOf(".") + 1);
        this.data = data;
        this.fullName = originalFilename;
    }

    public ZipMultiPartFile(String originalFilename,InputStream in) throws IOException{
        this(originalFilename,FileCopyUtils.copyToByteArray(in));
    }

    public String getFullName(){
        return fullName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getOriginalFilename() {
        return originalFilename;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public boolean isEmpty() {
        return data == null || data.length == 0;
    }

    @Override
    public long getSize() {
        return data == null ? 0 : data.length;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return data;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new ByteArrayInputStream(data);
    }

    @Override
    public void transferTo(File dest) throws IOException, IllegalStateException {
        FileCopyUtils.copy(data, dest);
    }

}
