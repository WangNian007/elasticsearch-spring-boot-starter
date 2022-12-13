package com.wang.es.starter.service;


import com.wang.es.starter.exception.EsOperationException;
import com.wang.es.starter.model.Page;
import com.wang.es.starter.model.PageParam;

import java.util.List;

/**
 * @author 王念
 * @create 2022-12-13 9:39
 */
public interface IEsService<T> {
    /**
     * 插入对象
     */
    T insert(T model, String index) throws Exception;

    /**
     * 插入对象,ifRefreshImmediate 为true 则立即刷新
     */
    T insert(T model, String index, Boolean ifRefreshImmediate) throws EsOperationException;


    /**
     * 批量插入 复用insert
     */
    List<T> inserts(List<T> models, String index) throws EsOperationException;

    /**
     * 批量插入 复用insert， ifRefreshImmediate 为true 则立即刷新
     */
    List<T> inserts(List<T> models, String index, Boolean ifRefreshImmediate) throws EsOperationException;

    /**
     * 更新对象
     */
    boolean update(T model, String index);

    /**
     * 更新对象
     */
    boolean update(T model, String index, Boolean ifRefreshImmediate);

    /**
     * 删除对象
     * 主键使用primaries会按,逗号分割主键以批量更新
     */
    boolean delete(T model, String index);

    /**
     * 批量删除
     * 循环复用delete方法
     * 若仅根据主键按逗号分割 更新数据相同delete方法即可支持
     * 用次方法目的仅在于复用delete方法更新数据体不同时
     */
    boolean deletes(List<T> model, String index);

    /**
     * 查询单个对象
     */
    T get(T model, String... index);

    /**
     * 查询分页方法
     */
    Page<T> page(PageParam page, T model, String... index);

    /**
     * Scroll 查询分页方法
     */
    Page<T> pageScroll(PageParam pageParam, T model, String... index);
}
