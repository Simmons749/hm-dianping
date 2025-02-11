package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.User;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    // 写入到数据库的线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    // 下单实现类
    private class VoucherOrderHandler implements Runnable {

        String queenName = "stream.orders";
        @Override
        public void run() {
            while (true) {

                try {
                    // 1.获取消息队列中的信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queenName, ReadOffset.lastConsumed())
                    );
                    // 2 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 2.2 如果获取成功，可以下单
                    // 2.2.1 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    handleVoucherOrder(voucherOrder);
                    // 3 返回ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queenName,"g1", record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常！", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {

                try {
                    // 1.获取pending-list中的信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queenName, ReadOffset.from("0"))
                    );
                    // 2 判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1 如果获取失败，说明pending-list没有异常消息，结束循环
                        break;
                    }
                    // 2.2 如果获取成功，可以下单
                    // 2.2.1 解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    handleVoucherOrder(voucherOrder);
                    // 3 返回ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queenName,"g1", record.getId());

                } catch (Exception e) {
                    log.error("处理pending-list异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }


    /**
     * JDK消息队列下单
     * @param voucherOrder
     */
    /*    // 阻塞队列
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    // 下单实现类
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {

                try {
                    // 1.获取阻塞队列中的信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2. 创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常！", e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1.获取用户
        Long userId = voucherOrder.getUserId();
        // 2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        // boolean isLock = lock.tryLock(1200);
        boolean isLock = lock.tryLock();
        // 获取代理对象(事务)
        if (!isLock) {
            // 获取锁失败，返回错误或重试
            log.error("不允许重复下单！");
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        // 2.判断结果是否为0
        if (result.intValue() != 0) {
            // 2.1 不为0，代表没有购买资格
            return Result.fail(result.intValue() == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.3 创建代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3 返回订单id
        return Result.ok(orderId);
    }

    /**
     * JDK实现消息队列
     */
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        // 2.判断结果是否为0
        if (result.intValue() != 0) {
            // 2.1 不为0，代表没有购买资格
            return Result.fail(result.intValue() == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2 为0，有购买资格， 把下单信息保存到阻塞队列中
        // 2.2.1 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 用户id
        voucherOrder.setUserId(userId);
        // 代金劵id
        voucherOrder.setVoucherId(voucherId);
        // 2.2.2 保存到阻塞队列中
        orderTasks.add(voucherOrder);

        // 2.3 创建代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3 返回订单id
        return Result.ok(orderId);
    }*/


    /**
     * 未实现秒杀优化原代码
     * @param voucherId
     * @return
     */
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 查询优惠卷信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始!");
        }
        // 判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 已经结束
            return Result.fail("秒杀已经结束!");
        }
        // 判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足!");
        }

        Long userId = UserHolder.getUser().getId();
//        synchronized (userId.toString().intern()) {
//            // 获取代理对象(事务)
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }

        // 分布式锁
        // 创建锁对象
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 获取锁
        // boolean isLock = lock.tryLock(1200);
        boolean isLock = lock.tryLock();
        // 获取代理对象(事务)
        if (!isLock) {
            // 获取锁失败，返回错误或重试
            return Result.fail("不允许重复下单!");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }

    }*/

    /**
     * 下单到数据库_未优化前
     */
    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 一人一单判断
        // 查询订单
        Long userId = UserHolder.getUser().getId();
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 判断是否存在
        if (count > 0) {
            // 用户已经购买过
            return Result.fail("用户已经购买过一次!");
        }

        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0) // CAS乐观锁
                .update();
        if (!success) {
            return Result.fail("库存不足!");
        }

        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 用户id
        voucherOrder.setUserId(userId);
        // 代金劵id
        voucherOrder.setVoucherId(voucherId);
        // 写入数据库
        save(voucherOrder);
        // 返回订单ID
        return Result.ok(orderId);
    }*/

    /**
     * 下单到数据库——优化后
     * @param
     * @return
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单判断
        // 查询订单
        Long userId = voucherOrder.getId();
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        // 判断是否存在
        if (count > 0) {
            // 用户已经购买过
            log.error("用户已经购买过！");
        }

        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0) // CAS乐观锁
                .update();
        if (!success) {
            log.error("库存不足！");
        }
        // 写入数据库
        save(voucherOrder);
    }
}
