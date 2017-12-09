package com.eq.handler.userinfo;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.cybermkd.mongo.kit.MongoQuery;
import com.eq.common.DataHandler;
import com.eq.common.RowRecord;
import com.eq.model.efun.EfunUserOrder;
import com.eq.model.order.Order;
import com.eq.model.user.UserInfo;
import com.eq.service.userinfo.OrderBuyService;
import com.eq.string.StringUtil;
import com.jfinal.aop.Duang;
import com.jfinal.kit.Ret;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class OrderBuyHandler implements DataHandler {
	private OrderBuyService orderBuyService = Duang.duang(OrderBuyService.class);

	@Override
	public boolean dealData(RowRecord rowRecord) {
		
		EventType eventType = rowRecord.getEventType();
        boolean needUpdate = false;
        Ret ret = Ret.create();
        String userId = null;
		MongoQuery mongoQuery = new MongoQuery();
		mongoQuery.use("allOrderBuy");
        if(EventType.INSERT.equals(eventType)) {
            Record after = rowRecord.getAfterRecord();
			after.set("mongoType",3);//这个类型代表不同的表 1.efun_user_order 2.order 3.order_buy
			orderBuyService.save(mongoQuery,after);
			//检查 买单表   当新增数据的时候   按照表单中user_id新增进 mongodb中

        }
		if(EventType.UPDATE.equals(eventType) )
		{
			Record after = rowRecord.getAfterRecord();
			Record updatedRecord = rowRecord.getUpdatedRecord();
            userId = after.getStr("user_id");
			orderBuyService.update(mongoQuery,after,updatedRecord);
		}
		return true;
	}
}
