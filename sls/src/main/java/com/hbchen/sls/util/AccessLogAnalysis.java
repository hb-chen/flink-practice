package com.hbchen.sls.util;

import java.io.Serializable;

public class AccessLogAnalysis implements Serializable {
    private String path;
    private Integer key;

    public AccessLogAnalysis() {
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }
}
