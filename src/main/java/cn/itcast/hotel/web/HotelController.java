package cn.itcast.hotel.web;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * @author 12141
 */
@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;

    /**
     * 搜索酒店数据
     *
     * @param requestParams
     * @return
     */
    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams requestParams){
        return hotelService.search(requestParams);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> filters(@RequestBody RequestParams requestParams){
        return hotelService.filters(requestParams);
    }

    @GetMapping("/suggestion")
    public List<String> suggestion(@RequestParam("key")String text){
        return hotelService.suggestion(text);
    }
}
