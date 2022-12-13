package com.wang.es.starter.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.wang.es.starter.code.ResultCode;
import com.wang.es.starter.exception.EsOperationException;
import com.wang.es.starter.model.Page;
import com.wang.es.starter.model.PageParam;
import com.wang.es.starter.pool.RestHighLevelClientPool;
import com.wang.es.starter.service.IEsService;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
        return this.insert(model, Boolean.FALSE, index);
    }

    @Override
    public T insert(T model, Boolean ifRefreshImmediate, String index) throws EsOperationException {
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
        return this.inserts(models, Boolean.FALSE, index);
    }

    @Override
    public List<T> inserts(List<T> models, Boolean ifRefreshImmediate, String index) throws EsOperationException {
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
    public boolean update(T model, String... indices) throws EsOperationException {
        return this.update(model, Boolean.FALSE, indices);
    }

    @Override
    public boolean update(T model, Boolean ifRefreshImmediate, String... indices) throws EsOperationException {
        RestHighLevelClient client = null;
        try {
            SearchRequest request = new SearchRequest(indices);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder query = this.getQuery(model, OperationType.LIST);
            if (query != null)
                searchSourceBuilder.query(query);
            searchSourceBuilder.size(1);
            request.source(searchSourceBuilder);
            client = pool.borrowObject();
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value == 1) {
                SearchHit hit = searchResponse.getHits().getAt(0);
                UpdateRequest updateRequest = new UpdateRequest(hit.getIndex(), hit.getId());
                updateRequest.doc(JSONObject.toJSONString(model), XContentType.JSON);
                if (ifRefreshImmediate) {
                    updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                }
                UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
                if (updateResponse.status() != RestStatus.OK) {
                    throw new EsOperationException(ResultCode.ERROR_UPDATE);
                }
            }
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_UPDATE);
        } finally {
            if (client != null)
                pool.returnObject(client);
        }

        return false;
    }

    @Override
    public boolean updateByQuery(T model, Script script, String... indices) throws EsOperationException {
        RestHighLevelClient client = null;
        try {
            UpdateByQueryRequest request =
                    new UpdateByQueryRequest(indices);
            request.setConflicts("proceed");
            QueryBuilder query = this.getQuery(model, OperationType.LIST);
            if (query != null)
                request.setQuery(query);
            request.setScript(script);
            request.setScroll(TimeValue.timeValueMinutes(10));
            request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            client = pool.borrowObject();
            final BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
            if (bulkByScrollResponse.getUpdated() <= 0) {
                return false;
            }
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_UPDATE);
        }
        return true;
    }

    @Override
    public boolean deleteByQuery(T model, String... indices) throws EsOperationException {
        RestHighLevelClient client;
        try {
            DeleteByQueryRequest request =
                    new DeleteByQueryRequest(indices);
            request.setConflicts("proceed");
            QueryBuilder query = this.getQuery(model, OperationType.LIST);
            if (query != null)
                request.setQuery(query);
            request.setScroll(TimeValue.timeValueMinutes(10));
            request.setRefresh(true);
            request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            client = pool.borrowObject();
            BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
            if (bulkByScrollResponse.getBulkFailures().size() > 0) {
                logger.error("error delete:{}", JSONObject.toJSONString(bulkByScrollResponse.getBulkFailures()));
                throw new EsOperationException(ResultCode.ERROR_DELETE);
            }
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_UPDATE);
        }
        return true;
    }

    @Override
    public boolean deletesByQuery(List<T> models, String... indices) throws EsOperationException {
        for (T model : models) {
            this.deleteByQuery(model, indices);
        }
        return true;
    }

    @Override
    public T get(T model, String... indices) {
        return null;
    }

    @Override
    public Page<T> page(PageParam page, T model, String... indices) {
        return null;
    }

    @Override
    public Page<T> pageScroll(PageParam pageParam, T model, String... indices) {
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
