#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-07 22:18:07
# Project: douyu_anchor

from pyspider.libs.base_handler import *
import re
import pymysql
from datetime import datetime

class Handler(BaseHandler):
    headers= {
    "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding":"gzip, deflate, sdch",
    "Accept-Language":"zh-CN,zh;q=0.8",
    "Cache-Control":"max-age=0",
    "Connection":"keep-alive",
    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36"
    }
    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }
    
    def __init__(self):
        self.limit = 20
        self.platform_id = 1
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        offset = 0
        try:
            cursor = self.connect.cursor()
            cursor.execute('select short_name,id from category where platform_id = 1;')
            results = cursor.fetchall()
            for item in results:
                self.crawl('http://capi.douyucdn.cn/api/v1/live/%s?&limit=%s&offset=%s' % (item[0], str(self.limit), str(offset),), callback=self.detail_page, save={
                           'offset': offset,
                           'short_name': item[0],
                           'category_id': item[1],
                           })
        except Exception as e:
            self.connect.rollback()
            raise e
        
        
        

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        pass
        
        
        
    @config(priority=2)
    def detail_page(self, response):
        if len(response.json['data']) == self.limit:
            save = response.save
            save['offset'] += 20
            self.crawl('http://capi.douyucdn.cn/api/v1/live/%s?&limit=%s&offset=%s' % (save['short_name'], str(self.limit), str(save['offset']),), callback=self.detail_page, save=save)
        return {
            "url": response.url,
            "results": response.json['data'],
            "category_id": response.save['category_id'],
        }
    
    def on_result(self,result):
        if not result:
            return
        self.save_data(**result)
        
    def save_data(self, **kw):

        if len(kw['results']) == 0:
            return

        for item in kw['results']:
            try:
                cursor = self.connect.cursor()
                cursor.execute('select id from anchor where user_id=%s and platform_id=%s', (item['owner_uid'],self.platform_id))
                result = cursor.fetchone()
                if result:
                    # 更新操作(是否创建个主播分析表（新爬虫？）：包含平台、主播id、)
                    sql = '''update anchor set 
                        name=%s, 
                        room_id=%s, 
                        room_name=%s, 
                        room_src=%s,
                        avatar=%s,
                        avatar_mid=%s,
                        avatar_small=%s,
                        fans=%s,
                        category_id=%s,
                        cate_id=%s,
                        online=%s,
                        pc_url=%s,
                        update_time=%s,
                        show_time=%s  
                        where user_id=%s and platform_id=%s'''
                    cursor.execute(sql, (item['nickname'],  
                                         item['room_id'], 
                                         item['room_name'], 
                                         item['room_src'], 
                                         item['avatar'], 
                                         item['avatar_mid'], 
                                         item['avatar_small'],  
                                         item['fans'],  
                                         kw['category_id'],  
                                         item['cate_id'],  
                                         item['online'], 
                                         'https://www.douyu.com/' + item['room_id'],
                                         datetime.now(), 
                                         datetime.fromtimestamp(float(item['show_time'])) if item['show_time'] else datetime.now(),
                                         item['owner_uid'], 
                                         self.platform_id))
                else:
                    # 插入操作
                    sql = '''insert into anchor(
                        user_id, 
                        name, 
                        room_id, 
                        room_name, 
                        room_src, 
                        avatar, 
                        avatar_mid, 
                        avatar_small, 
                        fans, 
                        category_id, 
                        cate_id, 
                        online, 
                        platform_id, 
                        pc_url,
                        show_time, 
                        created_time) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['owner_uid'], 
                                         item['nickname'], 
                                         item['room_id'], 
                                         item['room_name'], 
                                         item['room_src'],
                                         item['avatar'], 
                                         item['avatar_mid'], 
                                         item['avatar_small'], 
                                         item['fans'], 
                                         kw['category_id'], 
                                         item['cate_id'], 
                                         item['online'], 
                                         self.platform_id,
                                         'https://www.douyu.com/' + item['room_id'],
                                         datetime.fromtimestamp(float(item['show_time'])) if item['show_time'] else datetime.now(),
                                         datetime.now(),
                                        ))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
