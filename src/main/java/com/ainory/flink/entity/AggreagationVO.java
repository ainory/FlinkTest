package com.ainory.flink.entity;

import java.math.BigDecimal;

/**
 * @author ainory on 2018. 4. 4..
 */
public class AggreagationVO {
    private String key;
    private String hostname;
    private String plugin;
    private String plugin_instance;
    private String time;
    private String type;
    private String type_instance;
    private BigDecimal value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }

    public String getPlugin_instance() {
        return plugin_instance;
    }

    public void setPlugin_instance(String plugin_instance) {
        this.plugin_instance = plugin_instance;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType_instance() {
        return type_instance;
    }

    public void setType_instance(String type_instance) {
        this.type_instance = type_instance;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }
}
