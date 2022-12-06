package com.wang.es.starter.pool;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.DisposableBean;

/**
 * @author 王念
 * @create 2022-12-06 18:50
 */
public class RestHighLevelClientPool extends GenericObjectPool<RestHighLevelClient> implements DisposableBean {
    public RestHighLevelClientPool(PooledObjectFactory<RestHighLevelClient> factory) {
        super(factory);
    }

    public RestHighLevelClientPool(PooledObjectFactory<RestHighLevelClient> factory, GenericObjectPoolConfig<RestHighLevelClient> config) {
        super(factory, config);
    }

    @Override
    public void destroy() throws Exception {
        close();
    }
}
