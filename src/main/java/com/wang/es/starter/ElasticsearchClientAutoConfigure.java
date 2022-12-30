package com.wang.es.starter;

import com.wang.es.starter.config.ElasticsearchConfig;
import com.wang.es.starter.config.RestHighLevelClientPoolConfig;
import com.wang.es.starter.factory.RestHighLevelClientPooledFactory;
import com.wang.es.starter.pool.RestHighLevelClientPool;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author 王念
 * @create 2022-12-06 18:56
 */
@Configuration
@ConditionalOnClass({RestHighLevelClient.class, RestHighLevelClientPool.class, RestHighLevelClientPooledFactory.class})
public class ElasticsearchClientAutoConfigure {

    @Bean
    @ConfigurationProperties(prefix = ElasticsearchConfig.PREFIX)
    @ConditionalOnMissingBean(ElasticsearchConfig.class)
    public ElasticsearchConfig elasticsearchConfig() {
        return new ElasticsearchConfig();
    }

    @Bean
    @ConfigurationProperties(prefix = RestHighLevelClientPoolConfig.PREFIX)
    @ConditionalOnMissingBean(RestHighLevelClientPoolConfig.class)
    public RestHighLevelClientPoolConfig restHighLevelClientPoolConfig() {
        return new RestHighLevelClientPoolConfig();
    }

    @Bean
    @ConditionalOnMissingBean(RestHighLevelClientPooledFactory.class)
    public RestHighLevelClientPooledFactory restHighLevelClientPooledFactory(
            @Autowired ElasticsearchConfig elasticsearchConfig) {
        return new RestHighLevelClientPooledFactory(elasticsearchConfig);
    }

    @Bean
    @ConditionalOnMissingBean(RestHighLevelClientPool.class)
    public RestHighLevelClientPool restHighLevelClientPool(
            @Autowired RestHighLevelClientPooledFactory clientFactory,
            @Autowired RestHighLevelClientPoolConfig poolConfig) {
        poolConfig.setJmxEnabled(false);
        return new RestHighLevelClientPool(clientFactory, poolConfig);
    }
}
