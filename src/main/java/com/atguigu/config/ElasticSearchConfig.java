package com.atguigu.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @作者: zhulinjia
 * @时间: 2022/10/17
 */
@SpringBootConfiguration
@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticSearchConfig {

    private  String host;
    private  int port;


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public RestHighLevelClient client(){
        return new RestHighLevelClient(RestClient.builder(new HttpHost(host,port)));
    }
}
