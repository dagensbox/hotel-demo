package cn.itcast.hotel.pojo;

import lombok.Data;

/**
 * @author 12141
 */
@Data
public class RequestParams {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;
}
