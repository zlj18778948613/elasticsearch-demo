package com.atguigu.test;

import com.alibaba.fastjson.JSON;
import com.atguigu.domain.Goods;
import com.atguigu.dao.GoodsMapper;
import com.mysql.cj.QueryBindings;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @作者: zhulinjia
 * @时间: 2022/10/21
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchTest2 {

    @Autowired
    private GoodsMapper goodsMapper;

    @Autowired
    RestHighLevelClient client;

    @Test
    //批量导入数据
    public void  importData() throws IOException {
        List<Goods> goodsList = goodsMapper.findAll();

        BulkRequest bulkRequest = new BulkRequest();

        for (Goods goods:goodsList) {
            String specStr = goods.getSpecStr();

            Map map = JSON.parseObject(specStr, Map.class);

            goods.setSpec(map);

            //将goods对象转换为json字符串
            String data = JSON.toJSONString(goods);

            IndexRequest indexRequest = new IndexRequest("goods");
            indexRequest.id(goods.getId()+"").source(data, XContentType.JSON);

            bulkRequest.add(indexRequest);

        }

        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());

    }


    @Test
    public void testMatchAll() throws  IOException{
        //2.创建查询请求对象
        SearchRequest searchRequest = new SearchRequest("goods");

        //3.构建查询build对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //4.构建查询条件：查询所有
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        //5.指定分页信息
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.query(matchAllQueryBuilder);

        searchRequest.source(searchSourceBuilder);

        //1.通过高级客户端，执行查询所有请求，并分页
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println("searchResponse = " + searchResponse);

        //6.解析响应的结果
        SearchHits searchHits = searchResponse.getHits();
        long totalHits = searchHits.getTotalHits().value;
        System.out.println("总记录数 = " + totalHits);

        SearchHit[] hits = searchHits.getHits(); //真实数据部分

        List<Goods> list = new ArrayList<Goods>();

        //7.将es查询的结果数据封装成bean对象
        for (SearchHit hit : hits) {
            String sourceData = hit.getSourceAsString(); //json格式数据

            Goods goods = JSON.parseObject(sourceData,Goods.class);

            list.add(goods);
        }

        //8.打印数据结果
        for (Goods goods : list) {
            System.out.println("goods = " + goods);
        }
    }



}
