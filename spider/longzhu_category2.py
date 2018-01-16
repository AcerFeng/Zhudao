#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-16 15:51:27
# Project: longzhu_category2

from pyspider.libs.base_handler import *
import re
import pymysql
from datetime import datetime

class Handler(BaseHandler):
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Tga/4.6.2 Android(MI 6; Xiaomi/sagit/sagit:7.1.1/NMF26X/V9.2.3.0.NCACNEK:user/release-keys)',
        'x-b3-traceid': 'e03777d322ff245a99cdbf48a73a03d9',
        'x-b3-spanid': '562dfad582c05',
        'x-b3-sampled': 'true',
        'Host': 'stark.longzhu.com',
        'Connection': 'Keep-Alive',
        'Accept-Encoding': 'gzip',
        'Cookie': 'pluguest=C13778534B9F27CE4E73B9D87B474603CBBD183A880117D1D714DA0BDD9A8729651956E8B7AD2A68B07712696D12C05AC5D51ADD40B97EE3',
        'If-Modified-Since': 'Tue, 16 Jan 2018 06:47:44 GMT',
    }

    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }

    def __init__(self):
        self.platform_id = 5
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://stark.longzhu.com/api/v2/home/mobilegames?version=4.6.2&device=4&packageId=1&utm_sr=chanel_10',
                   callback=self.detail_page)

        self.crawl('https://stark.longzhu.com/api/v2/home/games?version=4.6.2&device=4&packageId=1&utm_sr=chanel_10',
                   callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        
        return {
            "url": response.url,
            "results": response.json['data']['slideIcons'],
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
                cursor.execute('select id from category where name=%s and platform_id=%s', (item['title'], self.platform_id))
                result = cursor.fetchone()
                if result:
                    # 更新操作
                    sql = '''update category set 
                        icon=%s, 
                        small_icon=%s,
                        cate_id=%s,
                        update_time=%s
                        where name=%s and platform_id=%s'''
                    cursor.execute(sql, (item['image'],
                                         item['image'],  
                                         item['target'], 
                                         datetime.now(),
                                         item['title'], 
                                         self.platform_id))
                else:
                    # 插入操作
                    sql = '''insert into category(name, icon, small_icon, cate_id, platform_id, created_time) 
                    values (%s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['title'], 
                                         item['image'], 
                                         item['image'], 
                                         item['target'], 
                                         self.platform_id,
                                         datetime.now(),))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e

