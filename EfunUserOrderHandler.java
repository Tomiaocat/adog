package com.eq.handler.userinfo;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.cybermkd.mongo.kit.MongoQuery;
import com.eq.common.DataHandler;
import com.eq.common.RowRecord;
import com.eq.model.efun.EfunUserOrder;
import com.eq.model.order.Order;
import com.eq.model.user.UserInfo;
import com.eq.service.userinfo.OrderBuyService;
import com.jfinal.aop.Duang;
import com.jfinal.kit.Ret;
import com.jfinal.plugin.activerecord.Db;
import com.jfinal.plugin.activerecord.Record;

public class EfunUserOrderHandler implements DataHandler {
	/**cpoy*/
	private OrderBuyService orderBuyService = Duang.duang(OrderBuyService.class);
	/**cpoy*/
	@Override
	public boolean dealData(RowRecord rowRecord) {
		
		EventType eventType = rowRecord.getEventType();
        boolean needUpdate = false;
        Ret ret = Ret.create();
        String userId = null;
		/**cpoy*/
		MongoQuery mongoQuery = new MongoQuery();
		mongoQuery.use("allOrderBuy");
		/**cpoy*/
        if(EventType.INSERT.equals(eventType)) {
            Record after = rowRecord.getAfterRecord();
            userId = after.getStr("user_id");

            lastOrderTime(eventType, userId, ret);
            needUpdate = true;
			/**cpoy*/
			if (after.getInt("efun_plus_type") == 2){
				after.set("mongoType",1);//这个类型代表不同的表 1.efun_user_order 2.order 3.order_buy
				orderBuyService.save(mongoQuery,after);
				System.out.println("=============================一折吃预付订单生成"+after.get("no"));
				//检查 买单表   当新增数据的时候   按照表单中user_id新增进 mongodb中
			}
			/**cpoy*/
        }
		if(EventType.UPDATE.equals(eventType) )
		{
			Record before = rowRecord.getBeforeRecord();
			Integer beforeStatus = before.getInt("status");
			Record after = rowRecord.getAfterRecord();
            userId = after.getStr("user_id");
			int status = after.getInt("status");

			if(EfunUserOrder.STATUS_NOT_FINISH_PAY==beforeStatus&&EfunUserOrder.STATUS_PAIED == status) {
				efunOrderPayNum(status, userId, ret);
				orderTotal(status, after.getInt("efun_plus_type"), userId, ret);
				 needUpdate = true;
			}
			/**cpoy*/
			if (after.getInt("efun_plus_type") == 2) {
				Record updatedRecord = rowRecord.getUpdatedRecord();
				after.set("mongoType",1);
				orderBuyService.update(mongoQuery, after, updatedRecord);
			}
			/**cpoy*/
		}
        if(needUpdate){
            ret.put("user_id", userId);
            UserInfo.dao.updateInfo(ret);
        }


		return true;
	}
	
	private void lastOrderTime(EventType eventType, String userId, Ret ret)
	{

			String sql = "select max(o.create_time) last_order_time from ( select euo.user_id, euo.create_time from t_efun_user_order euo where euo.user_id = ? union all select o.user_id, o.order_time create_time from t_order o where o.user_id = ? ) o group by o.user_id";
			ret.put("last_order_time", Db.queryDate(sql, userId, userId));

	}
	
	private void efunOrderPayNum(int status, String userId, Ret ret)
	{

		String sql = "select count(*) efun_order_pay_num from t_efun_user_order euo where euo.user_id = ? and euo.status = 1 and euo.efun_plus_type in (1, 2)";
		ret.put("efun_order_pay_num", Db.queryLong(sql, userId));

	}
	
	private void orderTotal(int status, int efunPlusType, String userId, Ret ret)
	{
			String sql = "select od_.efun_shop_order_total from t_user u left join ( select od.user_id, sum(od.total) efun_shop_order_total from ( select euo.user_id, sum(euo.total) total from t_efun_user_order euo where euo.user_id = ? and euo.status = ? and euo.efun_plus_type = ? union all select o.user_id, sum(o.total) total from t_order o where o.user_id = ? and o. status in (?, ?) and o.trade_status = ? and o.efun_plus_type = ? ) od group by od.user_id ) od_ on u.id = od_.user_id where u.id = ?";
			if(EfunUserOrder.EFUN_PLUS_ORDINARY == efunPlusType)
			{	
				ret.put("efun_shop_order_total", Db.queryBigDecimal(sql, userId, EfunUserOrder.STATUS_PAIED, EfunUserOrder.EFUN_PLUS_ORDINARY, userId, Order.STATUS_WAIT_FOR_EVALUATION, Order.STATUS_HAD_EVALUATION, Order.TRADE_NORMAL, EfunUserOrder.EFUN_PLUS_ORDINARY, userId));
			}
			else if(EfunUserOrder.EFUN_PLUS_EAT == efunPlusType)
			{
				ret.put("efun_eat_order_total", Db.queryBigDecimal(sql, userId, EfunUserOrder.STATUS_PAIED, EfunUserOrder.EFUN_PLUS_EAT, userId, Order.STATUS_WAIT_FOR_EVALUATION, Order.STATUS_HAD_EVALUATION, Order.TRADE_NORMAL, EfunUserOrder.EFUN_PLUS_EAT, userId));
			}
	}

}
