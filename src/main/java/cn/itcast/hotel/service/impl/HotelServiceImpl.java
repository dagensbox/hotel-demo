package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.DistanceUnit;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionBoostMode;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionScoreQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.JsonData;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 12141
 */
@Service
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private ElasticsearchClient client;

    @Override
    public PageResult search(RequestParams requestParams) {
        try {
            int page = requestParams.getPage();
            int size = requestParams.getSize();
            String location = requestParams.getLocation();
            if (StringUtils.isEmpty(location)) {
                Query query = getQuery(requestParams);
                SearchResponse<HotelDoc> searchResponse = client.search(builder ->
                        builder.index("hotel").query(query)
                                .from((page - 1) * size).size(size), HotelDoc.class);
                return getPageResult(searchResponse);
            } else {
                GeoLocation geoLocation = GeoLocation.of(builder -> builder.text(location));
                Query query = getQuery(requestParams);
                SearchResponse<HotelDoc> searchResponse = client.search(builder ->
                        builder.index("hotel").query(query)
                                .sort(builder1 -> builder1.geoDistance(builder2 ->
                                        builder2.field("location").location(geoLocation).order(SortOrder.Asc).unit(DistanceUnit.Kilometers)))
                                .from((page - 1) * size).size(size), HotelDoc.class);
                return getPageResult(searchResponse);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static PageResult getPageResult(SearchResponse<HotelDoc> searchResponse) {
        HitsMetadata<HotelDoc> metadata = searchResponse.hits();
        long total = metadata.total().value();
        List<HotelDoc> docList = metadata.hits().stream().map(hotelDocHit -> {
            HotelDoc source = hotelDocHit.source();
            List<String> sort = hotelDocHit.sort();
            if (sort != null && sort.size()>0){
                String sortValues = sort.get(0);
                source.setDistance(Double.valueOf(sortValues));
            }
            return source;
        }).collect(Collectors.toList());

        return new PageResult(total, docList);
    }

    private static Query getQuery(RequestParams requestParams) {
        String key = requestParams.getKey();
        String city = requestParams.getCity();
        String brand = requestParams.getBrand();
        String starName = requestParams.getStarName();
        Integer maxPrice = requestParams.getMaxPrice();
        Integer minPrice = requestParams.getMinPrice();

        //1、构建query集合
        List<Query> queries = new ArrayList<>();
        Query query;
        //2、关键字搜索
        if (StringUtils.isEmpty(key)) {
            query = Query.of(builder -> builder.matchAll(builder1 -> builder1));
        } else {
            query = Query.of(builder -> builder.match(builder1 -> builder1.field("all").query(key)));
        }
        queries.add(query);
        //3、城市条件
        if (!StringUtils.isEmpty(city)) {
            Query cityQuery = Query.of(builder -> builder.term(builder1 -> builder1.field("city").value(city)));
            queries.add(cityQuery);
        }
        //4、品牌条件
        if (!StringUtils.isEmpty(brand)) {
            Query brandQuery = Query.of(builder -> builder.term(builder1 -> builder1.field("brand").value(brand)));
            queries.add(brandQuery);
        }
        //5、星级条件
        if (!StringUtils.isEmpty(starName)) {
            Query starNameQuery = Query.of(builder -> builder.term(builder1 -> builder1.field("starName").value(starName)));
            queries.add(starNameQuery);
        }
        //6、价格条件
        if (minPrice != null && maxPrice != null) {
            Query priceQuery = Query.of(builder -> builder.range(builder1 -> builder1.field("price").lte(JsonData.of(maxPrice)).gte(JsonData.of(minPrice))));
            queries.add(priceQuery);
        }

        //7、放入最终query
        Query boolQuery = Query.of(builder -> builder.bool(builder1 -> builder1.must(queries)));
        FunctionScoreQuery functionScoreQuery = FunctionScoreQuery.of(builder -> builder.query(boolQuery).functions(
                builder1 -> builder1.filter(builder2 -> builder2.term(builder3 -> builder3.field("isAD").value(true))).weight(10.0)
        ).boostMode(FunctionBoostMode.Multiply));

        return Query.of(builder -> builder.functionScore(functionScoreQuery));
    }
}
