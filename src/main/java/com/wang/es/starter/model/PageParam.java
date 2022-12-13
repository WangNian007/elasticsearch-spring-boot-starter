package com.wang.es.starter.model;

import java.io.Serializable;

/**
 * @author 王念
 * @create 2022-12-13 9:58
 */
public class PageParam implements Serializable {
    private static final long serialVersionUID = 5619601636682299878L;

    /**
     * 页码 从1开始
     */

    private int pageNo = 1;

    /**
     * 每页条数 从10开始
     */

    private int pageSize = 10;

    /**
     * es scroll 翻页的 scrollId
     */
    private String scrollId;
    /**
     * es 查询指定某几个field
     */
    private String[] includeFields;
    /**
     *  es 查询指定不查询某几个field
     */
    private String[] excludeFields;

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public String[] getIncludeFields() {
        return includeFields;
    }

    public void setIncludeFields(String[] includeFields) {
        this.includeFields = includeFields;
    }

    public String[] getExcludeFields() {
        return excludeFields;
    }

    public void setExcludeFields(String[] excludeFields) {
        this.excludeFields = excludeFields;
    }
}
