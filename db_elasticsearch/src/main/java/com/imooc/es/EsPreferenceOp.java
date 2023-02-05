package com.imooc.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.Map;

/**
 * 偏好查询(分片查询方式)
 * Created by xuwei
 */
public class EsPreferenceOp {
    public static void main(String[] args) throws Exception{
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("bigdata01",9200,"http"),
                        new HttpHost("bigdata02",9200,"http"),
                        new HttpHost("bigdata03",9200,"http")));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("pre");

        //指定分片查询方式
        //searchRequest.preference();//默认随机
        //searchRequest.preference("_local");
        //searchRequest.preference("_only_local");
        //searchRequest.preference("_only_nodes:x21PnLd7TQa1ZsYSFaxoBw,sIwZPZM3TESKbWdjYsqgrA,_6ff1IjTTGm3jZ_edi6-KA");
        //searchRequest.preference("_prefer_nodes:x21PnLd7TQa1ZsYSFaxoBw");
        //searchRequest.preference("_shards:0,1");
        //searchRequest.preference("abc");//自定义参数

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
