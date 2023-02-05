package com.imooc.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

/**
 * routing路由功能的使用
 * Created by xuwei
 */
public class EsRoutingOp {
    public static void main(String[] args) throws Exception{
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("bigdata01",9200,"http"),
                        new HttpHost("bigdata02",9200,"http"),
                        new HttpHost("bigdata03",9200,"http")));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("rout");

        //指定分片查询方式
        //searchRequest.preference("_shards:0");

        //指定路由参数
        searchRequest.routing("class1");

        //执行查询操作
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取查询返回的结果
        SearchHits hits = searchResponse.getHits();
        //获取数据总量
        long numHits = hits.getTotalHits().value;
        System.out.println("数据总数："+numHits);
        //获取具体内容
        SearchHit[] searchHits = hits.getHits();
        //迭代解析具体内容
        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
        }

        //关闭连接
        client.close();
    }
}
