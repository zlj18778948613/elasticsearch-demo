package com.atguigu.test;

import com.alibaba.fastjson.JSON;
import com.atguigu.uilts.Person;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @作者: zhulinjia
 * @时间: 2022/10/17
 */


@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchTest {

    @Autowired
    private RestHighLevelClient client;

    @Test
    public void contextLoads() {

    }

    //创建索引
    @Test
    public void addIndex() throws IOException {
        //1.使用client对象获取索引对象
        IndicesClient indicesClient = client.indices();

        //2.创建请求头信息
        CreateIndexRequest request = new CreateIndexRequest("abc");

        CreateIndexResponse response = indicesClient.create(request, RequestOptions.DEFAULT);

        // 打印创建的返回结果
        System.out.println(response.isAcknowledged());

    }

    //创建索引和映射
    @Test
    public void addIndexAndMapper() throws IOException {
        //1.使用client对象获取索引对象
        IndicesClient indicesClient = client.indices();

        //2.创建请求头信息
        CreateIndexRequest request = new CreateIndexRequest("adc");

        //3.创建映射
        String mapping = "{\n" +
                "      \"properties\" : {\n" +
                "        \"address\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"analyzer\" : \"ik_max_word\"\n" +
                "        },\n" +
                "        \"age\" : {\n" +
                "          \"type\" : \"long\"\n" +
                "        },\n" +
                "        \"name\" : {\n" +
                "          \"type\" : \"keyword\"\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        request.mapping(mapping, XContentType.JSON);
        CreateIndexResponse response = indicesClient.create(request, RequestOptions.DEFAULT);
        // 打印创建的返回结果
        System.out.println(response.isAcknowledged());

    }

    //查询索引

    @Test
    public void queryIndex() throws  IOException{
        //创建索引对象
        IndicesClient indicesClient = client.indices();

        //创建请求头信息
        GetIndexRequest getIndexRequest = new GetIndexRequest("adc");

        //获取索引
        GetIndexResponse response = indicesClient.get(getIndexRequest, RequestOptions.DEFAULT);

        Map<String, MappingMetaData> mappings = response.getMappings();

        for (String key :mappings.keySet()) {
            System.out.println(key+":" + mappings.get(key).getSourceAsMap());
        }


    }

    @Test
    //删除索引
    public  void  deleIndex() throws IOException {
        //创建索引对象
        IndicesClient indices = client.indices();

        //创建删除请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("abc");

        AcknowledgedResponse response = indices.delete(deleteIndexRequest, RequestOptions.DEFAULT);

        System.out.println(response.isAcknowledged());

    }

    @Test
    //判断索引是否存在
    public void  existIndex() throws IOException {

        //创建索引对象
        IndicesClient indices = client.indices();
        //创建请求
        GetIndexRequest getIndexRequest = new GetIndexRequest("acc");

        boolean exists = indices.exists(getIndexRequest, RequestOptions.DEFAULT);

        System.out.println(exists);

    }

    //添加文档,使用map作为数据
    @Test
    public void addDoc() throws IOException {
        Map map = new HashMap<>();
        map.put("address","深圳宝安");
        map.put("name","尚硅谷");
        map.put("age",20);

        //创建索引对象
        IndicesClient indices = client.indices();

        //创建请求
        IndexRequest request = new IndexRequest("aaa").id("1").source(map);

        //添加数据，获取结果
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.getId());
    }

    @Test
    //添加文档,使用对象作为数据
    public void addDocPerson() throws IOException {
        Person person = new Person();
        person.setId("2");
        person.setName("硅谷2222");
        person.setAge(30);
        person.setAddress("北京昌平区");

        //要将对象进行转换成json格式
        String data = JSON.toJSONString(person);

        IndexRequest indexRequest = new IndexRequest("aaa").id(person.getId()).source(data,XContentType.JSON);

        IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println(index.getId());

    }
    @Test
    //修改文档
    public  void  updateDoc() throws IOException {
        Person  person =  new Person();
        person.setId("2");
        person.setAge(20);
        person.setName("朱林佳");
        person.setAddress("深圳");

        //将数据转换成json格式
        String data = JSON.toJSONString(person);

        //创建请求
        IndexRequest request = new IndexRequest("aaa").id(person.getId()).source(data, XContentType.JSON);

        //创建索引
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);

        System.out.println(index.getId());
    }

    @Test
   //根据id查询文档
    public  void  findDocById() throws IOException {
        GetRequest request = new GetRequest("aaa", "1");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        System.out.println(response.getSourceAsString());

    }

    @Test
    //根据ID进行文档删除
    public  void deleteDocById() throws IOException {
        DeleteRequest request = new DeleteRequest("aaa", "1");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);

        System.out.println(delete.getId());
        System.out.println(delete.getResult());
    }

    @Test
    public void  testBulk() throws IOException{
        //创建bulkrequest对象，整合所有操作
        BulkRequest bulkRequest = new BulkRequest();

        //删除记录
        DeleteRequest deleteRequest = new DeleteRequest("person");
        deleteRequest.id("6");
        bulkRequest.add(deleteRequest);

        //添加记录
        Map map = new HashMap<>();
        map.put("name","六号");

        IndexRequest indexRequest = new IndexRequest("person");

        indexRequest.id("5").source(map);
        bulkRequest.add(indexRequest);

        //修改3号记录，名称为"三号"

        Map map2 = new HashMap();
        map2.put("name","三号");


        UpdateRequest updateRequest = new UpdateRequest("person","2");
        updateRequest.doc(map2);

        bulkRequest.add(updateRequest);

        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());

    }



}
