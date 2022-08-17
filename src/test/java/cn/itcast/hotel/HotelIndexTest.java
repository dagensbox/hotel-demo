package cn.itcast.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class HotelIndexTest {

    private RestClient restClient;
    private ElasticsearchTransport transport;
    private ElasticsearchClient client;


    //创建索引 -不指定 mapping
    @Test
    void testCreateIndex() throws IOException {
        CreateIndexResponse indexResponse = client.indices().create(c -> c.index("test1"));
        System.out.println(indexResponse);
    }


    // 创建索引 -指定mapping
    @Test
    void testCreateIndexWithMapping() throws IOException {
        CreateIndexResponse indexResponse = client.indices()
                .create(c ->
                        c.index("test2").mappings(typeMapping ->
                                typeMapping.properties("name", objectBuilder ->
                                        objectBuilder.text(textProperty -> textProperty.fielddata(true))
                                ).properties("age", objectBuilder ->
                                        objectBuilder.integer(integerProperty -> integerProperty.index(true))
                                )
                        )
                );
        System.out.println(indexResponse);
    }


    // 创建索引 -指定mapping map
    @Test
    void testCreateIndexWithMappingMap() throws IOException {
        Map<String, Property> map = new HashMap<>();
        map.put("name", Property.of(property -> property.text(objectBuilder ->
                objectBuilder.fielddata(true).index(true).analyzer("ik_max_word"))));
        map.put("age", Property.of(property -> property.integer(objectBuilder ->
                objectBuilder)));
        CreateIndexResponse indexResponse = client.indices()
                .create(c -> c.index("test3").mappings(typeMapping -> typeMapping.properties(map)));
        System.out.println(indexResponse);
    }


    // 创建索引 -指定mapping map
    @Test
    void testCreateIndexOfHotel() throws IOException {
        CreateIndexResponse indexResponse = client.indices()
                .create(c -> c.index("hotel").mappings(typeMapping -> typeMapping.properties(getHotelMap())));
        System.out.println(indexResponse);
    }


    private Map<String, Property> getHotelMap() {
        Map<String, Property> map = new HashMap<>();
        map.put("id", Property.of(property -> property.keyword(objectBuilder ->
                objectBuilder)));
        map.put("name", Property.of(property -> property.text(objectBuilder ->
                objectBuilder.analyzer("ik_max_word").copyTo("all"))));
        map.put("address", Property.of(property -> property.keyword(objectBuilder ->
                objectBuilder.index(false))));
        map.put("price", Property.of(property -> property.integer(objectBuilder ->
                objectBuilder)));
        map.put("score", Property.of(property -> property.integer(objectBuilder ->
                objectBuilder)));
        map.put("brand", Property.of(property -> property.keyword(objectBuilder ->
                objectBuilder.copyTo("all"))));
        map.put("city", Property.of(property -> property.keyword(objectBuilder ->
                objectBuilder.copyTo("all"))));
        map.put("starName", Property.of(property -> property.keyword(objectBuilder ->
                objectBuilder)));
        map.put("business", Property.of(property -> property.keyword(objectBuilder ->
                objectBuilder)));
        map.put("location", Property.of(property -> property.geoPoint(objectBuilder ->
                objectBuilder)));
        map.put("pic", Property.of(property -> property.keyword(objectBuilder ->
                objectBuilder.index(false))));
        map.put("all", Property.of(property -> property.text(objectBuilder ->
                objectBuilder.analyzer("ik_max_word"))));
        return map;
    }

    // 删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexResponse response = client.indices().delete(c -> c.index("test1"));
        System.out.println(response);
    }

    //查看索引是否存在
    @Test
    void testExistsIndex() throws IOException {
        BooleanResponse response = client.indices().exists(c -> c.index("hotel"));
        System.out.println(response.value());
    }

    //查看索引
    @Test
    void testGetIndex() throws IOException {
        GetIndexResponse response = client.indices().get(c -> c.index("hotel"));
        Map<String, IndexState> result = response.result();
        result.forEach((k, v) -> System.err.println("key = " + k + ", value = " + v ));
    }

    //查看所有索引

    @Test
    void testGetAllIndex() throws IOException {
        IndicesResponse response = client.cat().indices();
        List<IndicesRecord> list = response.valueBody();
        list.forEach(System.out::println);
    }

    @Test
    void testInit() {
        System.out.println(client);
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
