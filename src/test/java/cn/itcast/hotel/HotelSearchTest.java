package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootTest
public class HotelSearchTest {
    @Autowired
    private IHotelService hotelService;

    private RestClient restClient;
    private ElasticsearchTransport transport;
    private ElasticsearchClient client;

    @Test
    void testMatchAll() throws IOException {
        SearchResponse<HotelDoc> searchResponse = client.search(req -> req.index("hotel").query(q -> q.matchAll(builder -> builder)), HotelDoc.class);
        // 解析响应
        handlerResponse(searchResponse);
    }

    @Test
    void testMatch() throws IOException {
        MatchQuery mq = MatchQuery.of(builder -> builder.field("all").query("如家"));
        SearchResponse<HotelDoc> searchResponse = client.search(req ->
                req.index("hotel").query(q -> q.match(mq)), HotelDoc.class);
        handlerResponse(searchResponse);
    }

    @Test
    void testBool() throws IOException {
        BoolQuery boolQuery = BoolQuery.of(builder -> builder.must(builder1 -> builder1.term(builder2 -> builder2.field("city").value("北京")))
                .filter(builder1 -> builder1.range(builder2 -> builder2.field("price").lte(JsonData.of(300)))));
        SearchResponse<HotelDoc> searchResponse = client.search(req ->
                req.index("hotel").query(q -> q.bool(boolQuery)), HotelDoc.class);
        handlerResponse(searchResponse);
    }


    @Test
    void testPageAndSort() throws IOException {
        int page = 2, size = 10;
        SearchRequest searchRequest = SearchRequest.of(builder -> builder.index("hotel").query(q -> q.matchAll(builder1 -> builder1))
                .sort(builder1 -> builder1.field(builder2 -> builder2.field("price").order(SortOrder.Asc)))
                .from((page-1)*size).size(size));
        SearchResponse<HotelDoc> searchResponse = client.search(searchRequest, HotelDoc.class);
        // 解析响应
        handlerResponse(searchResponse);
    }

    @Test
    void testHighlight() throws IOException {
        Query query = Query.of(builder -> builder.match(builder1 -> builder1.field("all").query("如家")));
        Highlight highlight = Highlight.of(builder -> builder.fields("name",builder1 -> builder1.requireFieldMatch(false)));
        SearchRequest searchRequest = SearchRequest.of(builder -> builder.index("hotel").query(query).highlight(highlight));
        SearchResponse<HotelDoc> searchResponse = client.search(searchRequest, HotelDoc.class);
        // 解析响应
        handlerResponse(searchResponse);
    }

    private static void handlerResponse(SearchResponse<HotelDoc> searchResponse) {
        // 解析响应
        HitsMetadata<HotelDoc> hitsMetadata = searchResponse.hits();
        // 获取总条数
        long total = hitsMetadata.total().value();
        System.out.println("共搜索到" + total + "条数据");
        //命中数据集合
        List<Hit<HotelDoc>> hitList = hitsMetadata.hits();
        for (Hit<HotelDoc> hit : hitList) {
            //获取原数据
            HotelDoc source = hit.source();
            //获取高粱结果
            Map<String, List<String>> listMap = hit.highlight();
            if (!CollectionUtils.isEmpty(listMap)){
                String name = listMap.get("name").get(0);
                source.setName(name);
            }
            System.out.println(source);
        }
    }

    @BeforeEach
    void setUp() {
        restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();
        // Create the transport with a Jackson mapper
        transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        // And create the API client
        client = new ElasticsearchClient(transport);
    }

    @AfterEach
    void tearDown() throws IOException {
        transport.close();
        restClient.close();
    }
}
