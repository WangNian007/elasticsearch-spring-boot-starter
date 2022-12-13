package com.wang.es.starter.exception;

import com.wang.es.starter.code.ResultCode;

/**
 * @author 王念
 * @create 2022-12-13 10:19
 */
public class EsOperationException extends Exception {
    private static final long serialVersionUID = -4325333207506827613L;

    public EsOperationException() {
        super();
    }

    /**
     * 异常码
     */
    private String code;
    /**
     * 描述型异常码
     */
    private String codeDesc;
    /**
     * 描述型异常码对应内容
     */
    private String[] descValue;// ["xxx"]

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getCodeDesc() {
        return codeDesc;
    }

    public void setCodeDesc(String codeDesc) {
        this.codeDesc = codeDesc;
    }

    public String[] getDescValue() {
        return descValue;
    }

    public void setDescValue(String[] descValue) {
        this.descValue = descValue;
    }

    public EsOperationException(String message) {
        super(message);
        this.setCode(message);
    }

    public EsOperationException(Throwable cause) {
        super(cause);
        if (isCausedBy(cause, EsOperationException.class)) {
            this.setCode(((EsOperationException) cause).getCode());
            this.setCodeDesc(((EsOperationException) cause).getCodeDesc());
            this.setDescValue(((EsOperationException) cause).getDescValue());
        } else {
            this.setCode(ResultCode.ERROR_INNER);
        }

    }

    public EsOperationException(String message, Throwable cause) {
        super(message, cause);
        this.setCode(message);
        if (isCausedBy(cause, EsOperationException.class)) {
            this.setCode(((EsOperationException) cause).getCode());
            this.setCodeDesc(((EsOperationException) cause).getCodeDesc());
            this.setDescValue(((EsOperationException) cause).getDescValue());
        }
    }

    public EsOperationException(String message, String descCode, String[] descValues) {
        super(message);
        this.setCode(message);
        this.setCodeDesc(descCode);
        this.setDescValue(descValues);
    }

    public EsOperationException(String message, String descCode, String[] descValues, Throwable cause) {
        super(message, cause);
        this.setCode(message);
        this.setCodeDesc(descCode);
        this.setDescValue(descValues);
    }

    public boolean isCausedBy(Exception ex, Class<? extends Exception>... causeExceptionClasses) {
        Throwable cause = ex.getCause();
        return isCausedBy(cause, causeExceptionClasses);
    }

    public boolean isCausedBy(Throwable cause, Class<? extends Exception>... causeExceptionClasses) {
        while (cause != null) {
            for (Class<? extends Exception> causeClass : causeExceptionClasses) {
                if (causeClass.isInstance(cause)) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}
