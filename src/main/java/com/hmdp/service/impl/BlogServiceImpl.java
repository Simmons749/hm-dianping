package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    /**
     * 根据博客id查询博客
     * @param id
     * @return
     */
    public Result queryBolgById(Long id) {
        // 查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        // 查询blog有关的用户
        queryBlogUser(blog);
        // 查询当前博客是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    /**
     * 查询当前博客是否被点赞
     * @param blog
     */
    private void isBlogLiked(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2.判断当前登录用户是否已经点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    /**
     * 查询热门帖子
     * @param current
     * @return
     */
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    /**
     * 点赞与取消点赞
     * @param id
     * @return
     */
    public Result likeBlog(Long id) {
        // 1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.判断当前登录用户是否已经点赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 3.如果未点赞，可以点赞
            // 3.1 数据库点赞加1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 3.2 保存用户到redis的zset集合
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 4.如果已点赞，取消点赞
            // 4.1 数据库点赞数减1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            // 4.2 把用户从redis的set集合中移除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();
    }

    /**
     * 点赞排行榜
     * @param id
     * @return
     */
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        // 1.查询top5的点赞用户（时间戳）
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 2.解析中其中的用户id
        List<Long> userIds = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        // 3.根据用户id查询用户
        // 默认使用in(),结果会按照id asc返回，有bug
        // 使用自定义query()实现
        String joinIds = StrUtil.join(",", userIds);
        List<UserDTO> userDTOS = userService.query()
                .in("id", userIds)
                .last("order by field(id," + joinIds + " )").list()
                .stream()
                .map(user ->
                        BeanUtil.copyProperties(user, UserDTO.class)
                ).collect(Collectors.toList());
        // 4.返回
        return Result.ok(userDTOS);
    }

    /**
     * 新增博客
     * @param blog
     * @return
     */
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("新增笔记失败!");
        }
        // 将新增的博客推送给该作者的粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        if (!follows.isEmpty()) {
            // 推送笔记id给所有粉丝
            for (Follow follow : follows) {
                // 获取粉丝id
                Long userId = follow.getUserId();
                // 推送
                String key = FEED_KEY + userId;
                stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());

            }
        }

        // 返回id
        return Result.ok(blog.getId());
    }

    /**
     * 滚动分页查询
     * @param max
     * @param offset
     * @return
     */
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询收件箱
        String key = FEED_KEY + userId;
        // 3.解析数据：blogId, score(时间戳), offset
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 4.根据id查询blog
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            // 4.1 获取id
            ids.add(Long.valueOf(tuple.getValue()));
            // 4.2 获取分数（时间戳）
            long time = tuple.getScore().longValue();
            if (time == minTime) {
                os ++ ;
            } else {
                minTime = time;
                os = 1;
            }
        }
        os = minTime == max ? os + offset : os;
        String strJoin = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id," + strJoin + ")").list();
        for (Blog blog : blogs) {
            // 查询blog有关的用户
            queryBlogUser(blog);
            // 查询当前博客是否被点赞
            isBlogLiked(blog);
        }

        // 5.封装并返回
        ScrollResult result = new ScrollResult();
        result.setList(blogs);
        result.setMinTime(minTime);
        result.setOffset(os);

        return Result.ok(result);
    }

    /**
     * 查询blog有关的用户信息
     * @param blog
     */
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
