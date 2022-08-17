package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService hotelService;

    private RestClient restClient;
    private ElasticsearchTransport transport;
    private ElasticsearchClient client;

    // 新增一条文档
    @Test
    void testAddDocument() throws IOException {
        // 根据id查询酒店数据
        Hotel hotel = hotelService.getById(60398);
        // 转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 发送请求
        IndexResponse response = client.index(request ->
                request.index("hotel").id(hotel.getId().toString()).document(hotelDoc)
        );
        System.out.println(response);
    }

    // 根据id查询文档
    @Test
    void testGetDocumentById() throws IOException {
        GetResponse<HotelDoc> response = client.get(request -> request.index("hotel").id("60398"), HotelDoc.class);
        System.out.println("id -->" + response.id());
        System.out.println("index -->" + response.index());
        System.out.println("routing -->" + response.routing());
        System.out.println("all -->" + response);
        HotelDoc source = response.source();
        System.out.println("-----------------------------");
        System.out.println(source);
    }

    //全量更新 即再写一次
    @Test
    void testUpdateDocAll() throws IOException {
        // 根据id查询酒店数据
        Hotel hotel = hotelService.getById(60398);
        // 转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        hotelDoc.setCity("重庆");
        // 发送请求
        IndexResponse response = client.index(request ->
                request.index("hotel").id(hotel.getId().toString()).document(hotelDoc)
        );
        System.out.println(response);
    }

    //全量更新 仅更新部分字段
    @Test
    void testUpdateDoc() throws IOException {
        HotelDoc hotelDoc = new HotelDoc();
        hotelDoc.setCity("New York");
        hotelDoc.setPrice(999);
        hotelDoc.setStarName("五钻石");
        UpdateResponse<HotelDoc> response = client.update(req ->
                        req.index("hotel").id(60398 + "").doc(hotelDoc)
                , HotelDoc.class);
        Result result = response.result();
        System.out.println(result);
        System.out.println(response);
    }

    //删除文档
    @Test
    void testDeleteById() throws IOException {
        DeleteResponse response = client.delete(req -> req.index("hotel").id("60398"));
        System.out.println(response);
    }


    // 批量导入文档到es
    @Test
    void testBulk() throws IOException {
        List<Hotel> hotelList = hotelService.list();
        List<BulkOperation> list = hotelList.stream().map(hotel -> {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            return new BulkOperation.Builder().create(createOperation -> createOperation.id(hotel.getId().toString()).document(hotelDoc)).build();
        }).collect(Collectors.toList());

        BulkResponse response = client.bulk(req -> req.index("hotel").operations(list));
        System.out.println(response);
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
