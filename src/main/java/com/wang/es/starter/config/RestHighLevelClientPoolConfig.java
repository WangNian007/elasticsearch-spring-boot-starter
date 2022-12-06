package com.wang.es.starter.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * @author 王念
 * @create 2022-12-06 18:52
 */
public class RestHighLevelClientPoolConfig extends GenericObjectPoolConfig<RestHighLevelClient> {
    public static final String PREFIX = "spring.es.pool";
    //初始化
    private static final boolean DEFAULT_CONNECTION_INIT = true;
    private boolean connectionInit = false;

    public RestHighLevelClientPoolConfig() {
        connectionInit = DEFAULT_CONNECTION_INIT;
    }

    public boolean getConnectionInit() {
        return connectionInit;
    }
    public boolean isConnectionInit() {
        return connectionInit;
    }

    public void setConnectionInit(boolean connectionInit) {
        this.connectionInit = connectionInit;
    }
}
