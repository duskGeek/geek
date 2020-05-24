package com.geek.sparksql.categoryUV

object CategorySql {

  val selectUV =
    """
      |SELECT
      |  view_uv.name AS category_name,
      |  view_uv.uv,
      |  deal_uv.pay_num
      |from
      |  (
      |    select
      |      name,
      |      count(distinct(open_id)) as uv
      |    from(
      |        SELECT
      |          t1.user_id as user_id,
      |          t1.open_id as open_id,
      |          REPLACE(if(t3.name IS NULL, t1.name, t3.name), '"', '') AS name
      |        FROM
      |          (
      |            SELECT
      |              user_id,
      |              open_id,
      |              get_json_object(buis_extend_para, '$.name') AS name,
      |              get_json_object(buis_extend_para, '$.categoryCode') AS categoryCode
      |            FROM
      |              rt_member_miniapp_flw_detail
      |            WHERE
      |              event_id IN (
      |                'xm00360500010010',
      |                'xm00360500010001',
      |                'xm00360100010001'
      |              )
      |              and time_local_date >= '2020-03-24'
      |          ) AS t1
      |          LEFT JOIN (
      |            SELECT
      |              code,
      |              root_code
      |            FROM
      |              member_goods_category
      |          ) AS t2 ON t1.categoryCode = t2.code
      |          LEFT JOIN (
      |            SELECT
      |              code,
      |              name
      |            FROM
      |              member_goods_category
      |          ) AS t3 ON t2.root_code = t3.code
      |        union all
      |        SELECT
      |          user_id,
      |          open_id,case
      |            when event_id in (
      |              'xm00200100010001',
      |              'xm00200200010001',
      |              'xm00200400010001'
      |            ) then '美食餐券'
      |            when event_id in (
      |              'xm00500100010001',
      |              'xm00500300010001',
      |              'xm00500400010001'
      |            ) then '音视频'
      |            when event_id in (
      |              'xm00800100010001',
      |              'xm00800200010001',
      |              'xm00800300010001',
      |              'xm00800400010001',
      |              'xm00800500010001'
      |            ) then '加油卡充值'
      |            when event_id in (
      |              'xm00100100020001',
      |              'xm00104000010001',
      |              'xm00103000020001'
      |            ) then '加油'
      |            when event_id in (
      |              'xm00700400010001',
      |              'xm00700100010001',
      |              'xm00700200010001',
      |              'xm00700300010001',
      |              'xm00700500010001'
      |            ) then '机场vip'
      |            when event_id in (
      |              'xm00600100010001',
      |              'xm00600100070001',
      |              'xm00600200010001',
      |              'xm00600300010001',
      |              'xm00600500010001'
      |            ) then '违章代办'
      |            when event_id in (
      |              'xm00500100010001',
      |              'xm00500300010001',
      |              'xm00500400010001'
      |            ) then '音视频'
      |            when event_id in (
      |              'xm00300100010001',
      |              'xm00300200010001',
      |              'xm00300300010001'
      |            ) then '洗车'
      |            when event_id in (
      |              'xm00000300010001',
      |              'xm00000300040001',
      |              'xm00000300050001',
      |              'xm00000300060001',
      |              'xm00000300120001'
      |            ) then '应急救援'
      |          end as name
      |        FROM
      |           rt_member_miniapp_flw_detail
      |        WHERE
      |          event_id IN (
      |            'xm00200100010001',
      |            'xm00200200010001',
      |            'xm00200400010001',
      |            -- 美食餐券
      |            'xm00500100010001',
      |            'xm00500300010001',
      |            'xm00500400010001',
      |            -- 音视频
      |            'xm00800100010001',
      |            'xm00800200010001',
      |            'xm00800300010001',
      |            'xm00800400010001',
      |            'xm00800500010001',
      |            -- 加油卡充值
      |            'xm00100100020001',
      |            'xm00104000010001',
      |            'xm00103000020001',
      |            -- 加油
      |            'xm00700400010001',
      |            'xm00700100010001',
      |            'xm00700200010001',
      |            'xm00700300010001',
      |            'xm00700500010001',
      |            -- 机场vip
      |            'xm00600100010001',
      |            'xm00600100070001',
      |            'xm00600200010001',
      |            'xm00600300010001',
      |            'xm00600500010001',
      |            -- 违章代办
      |            'xm00500100010001',
      |            'xm00500300010001',
      |            'xm00500400010001',
      |            -- 音视频
      |            'xm00300100010001',
      |            'xm00300200010001',
      |            'xm00300300010001',
      |            -- 洗车
      |            'xm00000300010001',
      |            'xm00000300040001',
      |            'xm00000300050001',
      |            'xm00000300060001',
      |            'xm00000300120001' -- 应急救援
      |          )
      |          and time_local_date >= '2020-03-24'
      |      ) as tt
      |    group by
      |      name
      |  ) view_uv
      |  JOIN (
      |    SELECT
      |      name,
      |      count(1) as create_num,
      |      count(if(is_pay = 1, 1, null)) as pay_num
      |    from
      |      (
      |        SELECT
      |          user_id,
      |          order_no,
      |          if(pay_time = create_date, 1, 0) AS is_pay,
      |          name
      |        FROM
      |          (
      |            SELECT
      |              t1.user_id AS user_id,
      |              t1.order_no AS order_no,
      |              t1.pay_time AS pay_time,
      |              t1.create_date AS create_date,
      |              t5.name AS name
      |            FROM
      |              (
      |                SELECT
      |                  user_id,
      |                  order_no,
      |                  substr(pay_time, 1, 10) AS pay_time,
      |                  substr(create_date, 1, 10) AS create_date
      |                FROM
      |                  mo_order_order_info
      |                WHERE
      |                  substr(create_date, 1, 10) >= '2020-03-24'
      |                  AND TYPE = '16'
      |              ) AS t1
      |              LEFT JOIN (
      |                SELECT
      |                  order_no,
      |                  goods_name,
      |                  sku_code
      |                FROM
      |                  mo_order_order_detail_info
      |              ) AS t2 ON t1.order_no = t2.order_no
      |              LEFT JOIN (
      |                SELECT
      |                  code,
      |                  spu_code
      |                FROM
      |                  member_goods_sku
      |              ) AS t3 ON t2.sku_code = t3.code
      |              LEFT JOIN (
      |                SELECT
      |                  code,
      |                  category_code
      |                FROM
      |                  member_goods_spu
      |              ) AS t6 ON t3.spu_code = t6.code
      |              LEFT JOIN (
      |                SELECT
      |                  code,
      |                  root_code
      |                FROM
      |                    member_goods_category
      |              ) AS t4 ON t6.category_code = t4.code
      |              LEFT JOIN (
      |                SELECT
      |                  code,
      |                  name
      |                FROM
      |                    member_goods_category
      |              ) AS t5 ON t4.root_code = t5.code
      |            UNION ALL
      |            SELECT
      |              user_id,
      |              order_no,
      |              substr(pay_time, 1, 10) AS pay_time,
      |              substr(create_date, 1, 10) AS create_date,
      |              case
      |                when type = 1 then '加油'
      |                when type = 2 then '洗车'
      |                when type = 3 then '保养'
      |                when type = 4 then '美食餐券'
      |                when type = 5 then '购买会员'
      |                when type = 6 then '音视频'
      |                when type = 7 then '违章代办'
      |                when type = 8 then '促销活动'
      |                when type = 9 then '机场vip'
      |                when type = 11 then '应急救援'
      |                when type = 12 then '实体卡'
      |                when type = 14 then '加油卡充值'
      |                when type = 15 then '拉新活动'
      |              end AS name
      |            FROM
      |               mo_order_order_info
      |            WHERE
      |              substr(create_date, 1, 10) >= '2020-03-24'
      |              AND TYPE not in ('13', '16')
      |          ) AS tt
      |      ) as ttt
      |    group by
      |      name
      |  ) deal_uv on deal_uv.name = view_uv.name
    """.stripMargin
}
