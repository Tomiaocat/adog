package com.eq.service.userinfo;

import com.alibaba.fastjson.JSONObject;
import com.cybermkd.mongo.kit.MongoQuery;
import com.eq.model.order.Order;
import com.eq.string.StringUtil;
import com.jfinal.plugin.activerecord.Record;
import com.sun.prism.impl.Disposer;

import java.math.BigDecimal;

/**
 * @author double on 2017/12/9 13:29.
 * @version 1.0
 */
public class OrderBuyService {
public void update(MongoQuery orderBuy, Record after ,Record updatedRecord) {
    //更新操作根据订单编号 查找 mongo中对应的数据
    JSONObject one = orderBuy.eq("no", after.getStr("no")).findOne();
    System.out.println(one);
    Long i = 10L;
    MongoQuery mongoQuery = new MongoQuery();
    MongoQuery allOrderList = mongoQuery.use("allOrderBuy");
    allOrderList.eq("no",after.getStr("no"));
    if (StringUtil.notNull(one)) {
        String[] columnNames = updatedRecord.getColumnNames();
        for (String s : columnNames) {
            if (updatedRecord.get(s)){
                if (    "price".equals(s)||"eq_price".equals(s)||
                        "actual_freight" .equals(s) ||
                        "original_amount".equals(s) ||
                        "prepaid_amount" .equals(s) ||
                        "store_freight"  .equals(s) ||
                        "actual_freight".equals(s)  || "store_freight".equals(s) || "freight".equals(s)||
                        "supplier_price".equals(s)  || "tax_rate".equals(s) || "prepaid_amount".equals(s)||
                        "cost".equals(s) || "cash".equals(s) || "coupon_discount".equals(s)||
                        "ev".equals(s)|| "total_discount".equals(s)|| "total".equals(s)|| "integral_discount".equals(s)){
                   i=allOrderList.modify(s,  (StringUtil.notNull(after.getBigDecimal(s)) ? after.getBigDecimal(s).intValue() : BigDecimal.ZERO)).update();
                }else{
                    i = allOrderList.modify(s, (Object) after.get(s)).update();
                }
            }
        }
        System.out.println(i);
    }else{
     save(orderBuy,after);
    }
}
    public void save(MongoQuery orderBuy, Record after){

            String[] columnNames = after.getColumnNames();
            for (String s : columnNames){
                if(!"freight".equals(s) && !"is_virtual".equals(s) && !"actual_freight".equals(s)){
                    orderBuy.set(s,after.get(s));
                }
            }
                orderBuy.set("cost", after.getBigDecimal("cost").intValue());
                orderBuy.set("cash", after.getBigDecimal("cash").intValue());
                orderBuy.set("coupon_discount", StringUtil.notNull(after.getBigDecimal("coupon_discount")) ? after.getBigDecimal("coupon_discount").intValue() : 0);
                orderBuy.set("ev", StringUtil.notNull(after.getBigDecimal("ev")) ? after.getBigDecimal("ev").intValue() : 0);
                orderBuy.set("total_discount", StringUtil.notNull(after.getBigDecimal("total_discount")) ? after.getBigDecimal("total_discount").intValue() : 0);
                orderBuy.set("total", StringUtil.notNull(after.getBigDecimal("total")) ? after.getBigDecimal("total").intValue() : 0);
                orderBuy.set("integral_discount", StringUtil.notNull(after.getBigDecimal("integral_discount")) ? after.getBigDecimal("integral_discount").intValue() : 0);
      if (after.getInt("mongoType") == 1){
                //一折预付部分
                orderBuy.set("eq_price",StringUtil.notNull(after.getBigDecimal("eq_price")) ? after.getBigDecimal("eq_price").intValue() : 0);
                orderBuy.set("tax_rate",StringUtil.notNull(after.getBigDecimal("tax_rate")) ? after.getBigDecimal("tax_rate").intValue() : 0);
                orderBuy.set("price",StringUtil.notNull(after.getBigDecimal("price")) ? after.getBigDecimal("price").intValue() : 0);
                orderBuy.set("supplier_price",StringUtil.notNull(after.getBigDecimal("supplier_price")) ? after.getBigDecimal("supplier_price").intValue() : 0);
                //订单decimal 数据类型进行处理部分
            }else if (after.getInt("mongoType") == 2) {
               // orderBuy.set("actual_freight"  , StringUtil.notNull(after.getBigDecimal("actual_freight")) ? after.getBigDecimal("actual_freight").intValue() : 0);
                orderBuy.set("original_amount", StringUtil.notNull(after.getBigDecimal("original_amount")) ? after.getBigDecimal("original_amount").intValue() : 0);
                orderBuy.set("prepaid_amount" , StringUtil.notNull(after.getBigDecimal("prepaid_amount")) ? after.getBigDecimal("prepaid_amount").intValue() : 0);
                orderBuy.set("store_freight"  , StringUtil.notNull(after.getBigDecimal("store_freight")) ? after.getBigDecimal("store_freight").intValue() : 0);
            }
            orderBuy.save();
    }
}