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
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * 聚合统计：统计每个学员的总成绩
 * Created by xuwei
 */
public class EsAggOp02 {
    public static void main(String[] args) throws Exception{
        //获取RestClient连接
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("bigdata01",9200,"http"),
                        new HttpHost("bigdata02",9200,"http"),
                        new HttpHost("bigdata03",9200,"http")));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("score");

        //指定查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("name_term")
                .field("name.keyword")//指定分组字段，如果是字符串(Text)类型，则需要指定使用keyword类型
                .subAggregation(AggregationBuilders.sum("sum_score").field("score"));//指定求sum，也支持avg、min、max等操作
        searchSourceBuilder.aggregation(aggregation);

        searchRequest.source(searchSourceBuilder);

        //执行查询操作
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //获取分组信息
        Terms terms = searchResponse.getAggregations().get("name_term");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket: buckets) {
            //获取sum聚合的结果
            Sum sum = bucket.getAggregations().get("sum_score");
            System.out.println(bucket.getKey()+"--"+sum.getValue());
        }
        //关闭连接
        client.close();
    }
}
