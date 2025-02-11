package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    /**
     * 根据博客id查询博客
     * @param id
     * @return
     */
    Result queryBolgById(Long id);

    /**
     * 查询热门博客
     * @param current
     * @return
     */
    Result queryHotBlog(Integer current);

    /**
     * 博客点赞
     * @param id
     * @return
     */
    Result likeBlog(Long id);

    /**
     * 点赞排行榜
     * @param id
     * @return
     */
    Result queryBlogLikes(Long id);
}
