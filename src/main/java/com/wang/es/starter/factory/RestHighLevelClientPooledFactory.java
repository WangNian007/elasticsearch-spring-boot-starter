package com.wang.es.starter.factory;

import com.wang.es.starter.config.ElasticsearchConfig;
import com.wang.es.starter.pool.RestHighLevelClientPool;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.DestroyMode;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author 王念
 * @create 2022-12-06 18:47
 */
public class RestHighLevelClientPooledFactory implements PooledObjectFactory<RestHighLevelClient> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RestHighLevelClientPooledFactory.class);
    private final ElasticsearchConfig config;

    public RestHighLevelClientPooledFactory(ElasticsearchConfig config) {
        this.config = config;
    }

    @Override
    public void activateObject(PooledObject<RestHighLevelClient> p) throws Exception {
        boolean result = false;
        try {
            result = p.getObject().ping(RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOGGER.debug("http pool active client ,ping result :{}", result);
        }
    }

    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> p) throws Exception {
        if (p.getObject() != null) {
            p.getObject().close();
        }
    }


    @Override
    public PooledObject<RestHighLevelClient> makeObject() throws Exception {
        HttpHost[] httpHosts = this.config.getHosts().stream()
                .map(host -> new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]), config.getSchema()))
                .toArray(HttpHost[]::new);
        final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        if (StringUtils.isNotEmpty(config.getUsername()) && StringUtils.isNotEmpty(config.getPassword())) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            restClientBuilder.setHttpClientConfigCallback((HttpAsyncClientBuilder httpAsyncClientBuilder) ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        return new DefaultPooledObject<>(new RestHighLevelClient(restClientBuilder));
    }

    @Override
    public void passivateObject(PooledObject<RestHighLevelClient> p) throws Exception {
    }

    @Override
    public boolean validateObject(PooledObject<RestHighLevelClient> p) {
        try {
            if (p.getObject() != null && p.getObject().ping(RequestOptions.DEFAULT)) {
                return true;
            }
        } catch (IOException e) {
            LOGGER.debug("es http client ping exception:{}", e.getMessage());
        }
        return false;
    }
}
