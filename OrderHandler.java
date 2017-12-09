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

public class OrderHandler implements DataHandler {
	/**
	 * cpoy
	 */
	private OrderBuyService orderBuyService = Duang.duang(OrderBuyService.class);

	/**
	 * cpoy
	 */
	@Override
	public boolean dealData(RowRecord rowRecord) {

/**cpoy*/
		MongoQuery mongoQuery = new MongoQuery();
		mongoQuery.use("allOrderBuy");
/**cpoy*/
		Record after = rowRecord.getAfterRecord();
		EventType eventType = rowRecord.getEventType();
		if (EventType.UPDATE.equals(eventType) || EventType.INSERT.equals(eventType)) {

			String userId = after.getStr("user_id");
			int status = after.getInt("status");
			int tradeStatus = after.getInt("trade_status");
			Ret ret = Ret.create();
			lastOrderTime(eventType, userId, ret);
			transacCompletNum(userId, status, tradeStatus, ret);
			orderTotal(status, after.getInt("efun_plus_type"), userId, ret);
			ret.put("user_id", userId);
			UserInfo.dao.updateInfo(ret);
		}
		/**cpoy*/
		if (after.getInt("efun_plus_type") == 2) {
			if (EventType.INSERT.equals(eventType)) {

				after.set("mongoType", 2);//这个类型代表不同的表 1.efun_user_order 2.order 3.order_buy
				orderBuyService.save(mongoQuery, after);
				//检查 买单表   当新增数据的时候   按照表单中user_id新增进 mongodb中

			}
			if (EventType.UPDATE.equals(eventType)) {

				Record updatedRecord = rowRecord.getUpdatedRecord();
				after.set("mongoType",2);
				orderBuyService.update(mongoQuery, after, updatedRecord);
			}
		}
			/**cpoy*/
			return true;
		}

	
	private void lastOrderTime(EventType eventType, String userId, Ret ret)
	{
		if(EventType.INSERT.equals(eventType))
		{
			String sql = "select max(o.create_time) last_order_time from ( select euo.user_id, euo.create_time from t_efun_user_order euo where euo.user_id = ? union all select o.user_id, o.order_time create_time from t_order o where o.user_id = ? ) o group by o.user_id";
			ret.put("last_order_time", Db.queryDate(sql, userId, userId));
		}
	}
	
	private void transacCompletNum(String userId, int status, int tradeStatus, Ret ret)
	{
		if(Order.STATUS_WAIT_FOR_EVALUATION == status || Order.STATUS_HAD_EVALUATION == status)
		{
		String sql = "select count(*) transac_complet_num from t_order o where o.user_id = ? and o.status in (3, 4) and o.trade_status = 0";
		ret.put("transac_complet_num", Db.queryLong(sql, userId));
		}
	}
	
	private void orderTotal(int status, int efunPlusType, String userId, Ret ret)
	{
		if(Order.STATUS_WAIT_FOR_EVALUATION == status || Order.STATUS_HAD_EVALUATION == status)
		{
			String sql = "select od_.efun_shop_order_total from t_user u left join ( select od.user_id, sum(od.total) efun_shop_order_total from ( select euo.user_id, sum(euo.total) total from t_efun_user_order euo where euo.user_id = ? and euo.status = ? and euo.efun_plus_type = ? union all select o.user_id, sum(o.total) total from t_order o where o.user_id = ? and o. status in (?, ?) and o.trade_status = ? and o.efun_plus_type = ? ) od group by od.user_id ) od_ on u.id = od_.user_id where u.id = ?";
			if(Order.EFUN_PLUS_ORDINARY == efunPlusType)
			{	
				ret.put("efun_shop_order_total", Db.queryBigDecimal(sql, userId, EfunUserOrder.STATUS_PAIED, Order.EFUN_PLUS_ORDINARY, userId, Order.STATUS_WAIT_FOR_EVALUATION, Order.STATUS_HAD_EVALUATION, Order.TRADE_NORMAL, Order.EFUN_PLUS_ORDINARY, userId));
			}
			else if(Order.EFUN_PLUS_EAT == efunPlusType)
			{
				ret.put("efun_eat_order_total", Db.queryBigDecimal(sql, userId, EfunUserOrder.STATUS_PAIED, Order.EFUN_PLUS_EAT, userId, Order.STATUS_WAIT_FOR_EVALUATION, Order.STATUS_HAD_EVALUATION, Order.TRADE_NORMAL, Order.EFUN_PLUS_EAT, userId));
			}
		}
	}

}
