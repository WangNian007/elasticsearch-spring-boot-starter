package com.wang.es.starter.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wang.commons.code.ResultCode;
import com.wang.commons.domain.Page;
import com.wang.commons.domain.PageImpl;
import com.wang.commons.domain.Pageable;
import com.wang.commons.enums.EsOperateType;
import com.wang.commons.exception.EsOperationException;
import com.wang.commons.service.IEsService;
import com.wang.commons.utils.ConvertUtils;
import com.wang.es.starter.pool.RestHighLevelClientPool;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * @author 王念
 * @create 2022-12-13 10:01
 */
public abstract class GenericEsService<T, ID> implements IEsService<T, ID> {
    private static final Logger logger = LoggerFactory.getLogger(GenericEsService.class);
    @Autowired
    private RestHighLevelClientPool pool;

    public abstract QueryBuilder getQuery(T model, EsOperateType EsOperateType);

    public abstract Iterable<SortBuilder<FieldSortBuilder>> getSort(T model, EsOperateType EsOperateType);

    @Override
    public T insert(T entity, String index) throws EsOperationException {
        return this.insert(entity, Boolean.FALSE, index);
    }

    @Override
    public T insert(T entity, Boolean ifRefreshImmediate, String index) throws EsOperationException {
        RestHighLevelClient restClient = null;
        try {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest(index).opType(DocWriteRequest.OpType.INDEX)
                    .source(JSONObject.toJSONString(entity), XContentType.JSON));
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
        return entity;
    }

    @Override
    public Iterable<T> inserts(Iterable<T> iterable, String index) throws EsOperationException {
        return this.inserts(iterable, Boolean.FALSE, index);
    }

    @Override
    public Iterable<T> inserts(Iterable<T> iterable, Boolean ifRefreshImmediate, String index) throws EsOperationException {
        RestHighLevelClient client = null;
        try {
            BulkRequest request = new BulkRequest();
            for (T t : iterable) {
                request.add(new IndexRequest(index).opType(DocWriteRequest.OpType.INDEX)
                        .source(JSONObject.toJSONString(t), XContentType.JSON));
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
        return iterable;
    }

    @Override
    public boolean delete(ID id, String index) throws EsOperationException {
        return this.delete(id, Boolean.FALSE, index);
    }

    @Override
    public boolean delete(ID id, Boolean ifRefreshImmediate, String index) throws EsOperationException {
        if (id == null) {
            throw new IllegalArgumentException("id must not be null!");
        }
        RestHighLevelClient client = null;
        try {
            DeleteRequest request = new DeleteRequest(
                    index,
                    ConvertUtils.stringIdRepresentation(id));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            if (ifRefreshImmediate) {
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            client = pool.borrowObject();
            client.delete(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_SAVE);
        } finally {
            if (client != null)
                pool.returnObject(client);
        }
        return true;
    }

    @Override
    public boolean deleteByQuery(T entity, String... indices) throws EsOperationException {
        RestHighLevelClient client;
        try {
            DeleteByQueryRequest request =
                    new DeleteByQueryRequest(indices);
            request.setConflicts("proceed");
            QueryBuilder query = this.getQuery(entity, EsOperateType.LIST);
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
    public boolean update(ID id, T entity, String index) throws EsOperationException {
        return this.update(id, entity, Boolean.FALSE, index);
    }

    @Override
    public boolean update(ID id, T entity, Boolean ifRefreshImmediate, String index) throws EsOperationException {
        RestHighLevelClient client = null;
        if (id == null) {
            throw new IllegalArgumentException("id must not be null!");
        }
        try {
            UpdateRequest request = new UpdateRequest(
                    index,
                    ConvertUtils.stringIdRepresentation(id));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            if (ifRefreshImmediate) {
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            request.doc(JSONObject.toJSONString(entity), XContentType.JSON);
            client = pool.borrowObject();
            client.update(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_UPDATE);
        } finally {
            if (client != null)
                pool.returnObject(client);
        }
        return true;
    }

    @Override
    public boolean updateByQuery(T entity, String script, String... indices) throws EsOperationException {
        return this.updateByQuery(entity, script, Boolean.FALSE, indices);
    }

    @Override
    public boolean updateByQuery(T entity, String script, Boolean ifRefreshImmediate, String... indices) throws EsOperationException {
        RestHighLevelClient client = null;
        if (StringUtils.isEmpty(script)) {
            throw new IllegalArgumentException("script must be not null!");
        }
        try {
            UpdateByQueryRequest request =
                    new UpdateByQueryRequest(indices);
            request.setConflicts("proceed");
            final QueryBuilder query = this.getQuery(entity, EsOperateType.LIST);
            if (query != null)
                request.setQuery(query);
            //默认一千，可更改
            request.setBatchSize(1000);
            request.setScript(
                    new Script(
                            ScriptType.INLINE, "painless",
                            script,
                            Collections.emptyMap()));
            client = pool.borrowObject();
            client.updateByQuery(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_UPDATE);
        } finally {
            if (client != null)
                pool.returnObject(client);
        }

        return true;
    }

    @Override
    public T get(ID id, String index) throws EsOperationException {
        return this.get(id, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY, index);
    }


    @Override
    public T get(ID id, String[] includes, String[] excludes, String index) throws EsOperationException {
        RestHighLevelClient client = null;
        if (id == null) {
            throw new IllegalArgumentException("id must be not null!");
        }
        try {
            GetRequest getRequest = new GetRequest(
                    index,
                    ConvertUtils.stringIdRepresentation(id));
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            getRequest.fetchSourceContext(fetchSourceContext);
            getRequest.refresh(true);
            client = pool.borrowObject();
            GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                long version = getResponse.getVersion();
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                sourceAsMap.put("esId", getResponse.getId());
                sourceAsMap.put("version", version);
                return (T) JSONObject.parseObject(JSON.toJSONString(sourceAsMap));
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_SEARCH);
        } finally {
            if (client != null) {
                pool.returnObject(client);
            }
        }
    }

    @Override
    public Page<T> page(T entity, Pageable pageable, String... indices) throws EsOperationException {
        RestHighLevelClient client = null;
        PageImpl<T> pageResult = null;
        try {
            SearchRequest request = new SearchRequest(indices);
            final QueryBuilder query = this.getQuery(entity, EsOperateType.LIST);
            final Iterable<SortBuilder<FieldSortBuilder>> sortBuilderIterable = this.getSort(entity, EsOperateType.LIST);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            if (query != null)
                searchSourceBuilder.query(query);
            searchSourceBuilder.fetchSource(pageable.getIncludeFields(), pageable.getExcludeFields());
            for (SortBuilder<FieldSortBuilder> sortBuilder : sortBuilderIterable) {
                searchSourceBuilder.sort(sortBuilder);
            }
            searchSourceBuilder.size(pageable.getPageSize()); //设定每次返回多少条数据
            searchSourceBuilder.from((pageable.getPageNumber() - 1) * pageable.getPageSize());
            request.source(searchSourceBuilder);
            pageResult = new PageImpl<>(new ArrayList<>(), pageable);
            client = pool.borrowObject();
            final SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            if (searchHits.length == 0) {
                return pageResult;
            }
            for (SearchHit searchHit : searchHits) {
                Map<String, Object> map = searchHit.getSourceAsMap();
                if (MapUtils.isNotEmpty(map)) {
                    map.put("_id", searchHit.getId());
                    map.put("_index", searchHit.getIndex());
                }
                pageResult.getContent().add((T) JSON.parseObject(JSON.toJSONString(map), entity.getClass()));
            }
        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_SEARCH);
        } finally {
            if (client != null)
                pool.returnObject(client);
        }
        return pageResult;
    }

    @Override
    public boolean exists(ID id, String index) throws EsOperationException {
        RestHighLevelClient client = null;
        if (id == null) {
            throw new IllegalArgumentException("id must be not null!");
        }
        boolean exists = false;
        try {
            GetRequest getRequest = new GetRequest(
                    index,
                    ConvertUtils.stringIdRepresentation(id));
            getRequest.fetchSourceContext(new FetchSourceContext(false));
            getRequest.storedFields("_none_");
            client = pool.borrowObject();
            exists = client.exists(getRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            logger.error("error save", e);
            throw new EsOperationException(ResultCode.ERROR_SEARCH);
        } finally {
            if (client != null)
                pool.returnObject(client);
        }
        return exists;
    }
}
