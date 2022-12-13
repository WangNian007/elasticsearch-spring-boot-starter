package com.wang.es.starter.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.wang.es.starter.code.ResultCode;
import com.wang.es.starter.exception.EsOperationException;
import com.wang.es.starter.model.Page;
import com.wang.es.starter.model.PageParam;
import com.wang.es.starter.pool.RestHighLevelClientPool;
import com.wang.es.starter.service.IEsService;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author 王念
 * @create 2022-12-13 10:01
 */
public abstract class GenericEsService<T> implements IEsService<T> {
    private static final Logger logger = LoggerFactory.getLogger(GenericEsService.class);
    @Autowired
    private RestHighLevelClientPool pool;

    public abstract QueryBuilder getQuery(T model, OperationType operationType);

    @Override
    public T insert(T model, String index) throws EsOperationException {
        return this.insert(model, index, Boolean.FALSE);
    }

    @Override
    public T insert(T model, String index, Boolean ifRefreshImmediate) throws EsOperationException {
        RestHighLevelClient restClient = null;
        try {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest(index).opType(DocWriteRequest.OpType.INDEX)
                    .source(JSONObject.toJSONString(model), XContentType.JSON));
            if (ifRefreshImmediate) {
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            restClient = pool.borrowObject();
            BulkResponse bulkResponse = restClient.bulk(request, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                logger.error("error.save " + bulkResponse.buildFailureMessage());
                throw new EsOperationException(ResultCode.ERROR_SAVE);
            }
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_SAVE);
        } finally {
            if (restClient != null) {
                pool.returnObject(restClient);
            }
        }
        return model;
    }

    @Override
    public List<T> inserts(List<T> models, String index) throws EsOperationException {
        return this.inserts(models, index, Boolean.FALSE);
    }

    @Override
    public List<T> inserts(List<T> models, String index, Boolean ifRefreshImmediate) throws EsOperationException {
        RestHighLevelClient client = null;
        try {
            BulkRequest request = new BulkRequest();
            for (T model : models) {
                request.add(new IndexRequest(index).opType(DocWriteRequest.OpType.INDEX)
                        .source(JSONObject.toJSONString(model), XContentType.JSON));
            }
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            if (ifRefreshImmediate) {
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            client = pool.borrowObject();
            final BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            if (bulkResponse.hasFailures()) {
                logger.error("error.save " + bulkResponse.buildFailureMessage());
                throw new EsOperationException(ResultCode.ERROR_SAVE);
            }
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_SAVE);
        } finally {
            if (client != null)
                pool.returnObject(client);
        }
        return models;
    }

    @Override
    public boolean update(T model, String index) {
        return false;
    }

    @Override
    public boolean update(T model, String index, Boolean ifRefreshImmediate) {
        return false;
    }

    @Override
    public boolean delete(T model, String index) {
        return false;
    }

    @Override
    public boolean deletes(List<T> model, String index) {
        return false;
    }

    @Override
    public T get(T model, String... index) {
        return null;
    }

    @Override
    public Page<T> page(PageParam page, T model, String... index) {
        return null;
    }

    @Override
    public Page<T> pageScroll(PageParam pageParam, T model, String... index) {
        return null;
    }

    public enum OperationType {
        GET(1, "GET"),
        UPDATE(2, "UPDATE"),
        DELETE(3, "DELETE"),
        LIST(4, "LIST");

        private Integer code;
        private String desc;

        OperationType(Integer code, String desc) {
            this.code = code;
            this.desc = desc;
        }

        public Integer getCode() {
            return this.code;
        }

        public void setCode(Integer code) {
            this.code = code;
        }

        public String getDesc() {
            return this.desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }
    }
}
