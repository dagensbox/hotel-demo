package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
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
            String key = requestParams.getKey();
            int page = requestParams.getPage();
            int size = requestParams.getSize();

            Query query;
            if (StringUtils.isEmpty(key)) {
                query = Query.of(builder -> builder.matchAll(builder1 -> builder1));
            } else {
                query = Query.of(builder -> builder.match(builder1 -> builder1.field("all").query(key)));
            }

            SearchResponse<HotelDoc> searchResponse = client.search(builder ->
                    builder.index("hotel").query(query).from((page - 1) * size).size(size), HotelDoc.class);
            HitsMetadata<HotelDoc> metadata = searchResponse.hits();
            long total = metadata.total().value();
            List<HotelDoc> docList = metadata.hits().stream().map(Hit::source).collect(Collectors.toList());

            return new PageResult(total, docList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
