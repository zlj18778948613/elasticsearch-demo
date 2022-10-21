package com.atguigu.test;

import com.alibaba.fastjson.JSON;
import com.atguigu.domain.Goods;
import com.atguigu.dao.GoodsMapper;
import com.mysql.cj.QueryBindings;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
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

    @Test
    public void testTermQuery() throws IOException{
        //创建查询对象
        SearchRequest searchRequest = new SearchRequest("goods");
        //创建查询Bulider对象
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "华为");
        sourceBuilder.query(termQueryBuilder);

        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取查询的内容
        SearchHits hits = response.getHits();

        //获取记录数
        TotalHits totalHits = hits.getTotalHits();
        System.out.println("总记录数:"+totalHits);

        //设置一个list对象进行存储
        ArrayList<Goods> list = new ArrayList<>();
        SearchHit[] hitsHits = hits.getHits();

        for (SearchHit hit:hitsHits) {
            String sourceAsString = hit.getSourceAsString();
            //将JSON格式转换对GOODS对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);
            list.add(goods);
        }

        //对list进行循环遍历
        for (Goods goods:list) {
            System.out.println(goods);
        }

    }

    //模糊查询
    @Test
    public void testWildcardQuery() throws  IOException{
        SearchRequest searchRequest = new SearchRequest("goods");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("title", "华**");

        sourceBuilder.query(wildcardQueryBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();

        //获取总记录数据
        long totalHits = hits.getTotalHits().value;
        SearchHit[] hitsHits = hits.getHits();

        //创建一个list对象进行存储
        List<Goods> list = new ArrayList<>();

        for (SearchHit hit :hitsHits) {
            String sourceAsString = hit.getSourceAsString();
            //转换成GOODS对象
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);

            list.add(goods);
        }

        for (Goods goods:list) {
            System.out.println(goods);
        }
    }


    @Test //前缀模糊查询
    public  void  testPrefixQuery() throws IOException {
        //创建索引对象
        SearchRequest searchRequest = new SearchRequest("goods");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        PrefixQueryBuilder queryBuilder = QueryBuilders.prefixQuery("brandName", "三");

        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();

        //获取总记录数
        long value = hits.getTotalHits().value;

        System.out.println("总记录数:"+value);
        SearchHit[] searchHits = hits.getHits();

        //创建list对象进行存储
        List<Goods> list = new ArrayList();

        for (SearchHit hit: searchHits) {
            String sourceAsString = hit.getSourceAsString();
            Goods goods = JSON.parseObject(sourceAsString, Goods.class);
            list.add(goods);

        }
        for (Goods goods:list) {
            System.out.println(goods);
        }


    }


}
