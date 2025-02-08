package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 查询redis缓存
        String key = "SHOP_TYPE";
        String shopType = stringRedisTemplate.opsForValue().get(key);
        // 如果有，返回
        if (StrUtil.isNotBlank(shopType)) {
            List<List> shopTypeList = JSONUtil.toList(shopType, List.class);
            return Result.ok(shopTypeList);
        }
        // 没有，查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        // 如果数据库为空，直接返回
        if (shopTypeList.isEmpty()) {
            return Result.fail("未查询到商铺信息!");
        }
        // 不为空，放入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypeList));

        return Result.ok(shopTypeList);
    }
}
