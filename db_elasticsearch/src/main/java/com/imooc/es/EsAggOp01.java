package com.imooc.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * 聚合统计：针对相同年龄的学员个数
 * Created by xuwei
 */
public class EsAggOp01 {
    public static void main(String[] args) throws Exception{
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("bigdata01",9200,"http"),
                        new HttpHost("bigdata02",9200,"http"),
                        new HttpHost("bigdata03",9200,"http")));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("stu");

        //指定查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //指定分组信息，默认是执行count聚合
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("age_term")
                .field("age")
                .size(Integer.MAX_VALUE);//获取指定分组个数的数据
        searchSourceBuilder.aggregation(aggregation);

        //增加分页参数，注意：分页参数针对分组数据是无效的
        //searchSourceBuilder.from(0).size(20);

        searchRequest.source(searchSourceBuilder);

        //执行查询操作
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取分组信息
        Terms terms = searchResponse.getAggregations().get("age_term");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket: buckets) {
            System.out.println(bucket.getKey()+"---"+bucket.getDocCount());
        }

        //关闭连接
        client.close();
    }
}
