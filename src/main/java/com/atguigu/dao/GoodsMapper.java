package com.atguigu.dao;

import com.atguigu.domain.Goods;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @作者: zhulinjia
 * @时间: 2022/10/21
 */

@Repository
@Mapper
public interface GoodsMapper {
    public List<Goods> findAll();
}
