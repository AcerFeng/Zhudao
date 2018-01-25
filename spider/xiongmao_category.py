#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-12-26 00:28:17
# Project: xiongmao_category

from pyspider.libs.base_handler import *
import pymysql
from datetime import datetime
import json
import time


class Handler(BaseHandler):
    headers = {
        ':authority': 'api.m.panda.tv',
        ':method': 'GET',
        ':path': '/index.php?method=category.list&type=game',
        ':scheme': 'https',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cookie': 'pdftv1=a8f8e|160f8d4a8b1|20a6|b70e8684|f; __guid=96554777.2421261672821548500.1516864650623.5828; Hm_lvt_204071a8b1d0b2a04c782c44b88eb996=1516864652,1516866660; Hm_lpvt_204071a8b1d0b2a04c782c44b88eb996=1516866660',
        'origin': 'https://m.panda.tv',
        'referer': 'https://m.panda.tv/type.html?pdt=2.1.h.3.6kkcphjg7vc',
        'user-agent': 'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Mobile Safari/537.36',
    }
    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }

    def __init__(self):
        self.platform_id = 3
        try:
            self.connect = pymysql.connect(
                host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')

        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://api.m.panda.tv/index.php?method=category.list&type=game',
                   callback=self.detail_page)
        self.crawl('https://api.m.panda.tv/index.php?method=category.list&type=yl&__plat=ios',
                   callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        return {
            "url": response.url,
            "title": response.doc('title').text(),
            "results": response.json['data']
        }

    def on_result(self, result):
        if not result:
            return
        self.save_data(**result)

    def save_data(self, **kw):

        if len(kw['results']) == 0:
            return

        for item in kw['results']:
            extra_fan = json.loads(item['extra'])
            try:
                cursor = self.connect.cursor()
                cursor.execute(
                    'select id from category where short_name=%s and platform_id=%s', (item['ename'], self.platform_id))
                result = cursor.fetchone()
                if result:
                    # 更新操作
                    sql = '''update category set 
                        name=%s, 
                        pic=%s, 
                        icon=%s, 
                        small_icon=%s,
                        mb_url=%s,
                        pc_url=%s,
                        update_time=%s
                        where short_name=%s and platform_id=%s'''
                    cursor.execute(sql, (item['cname'],
                                         item['img'],
                                         extra_fan['icon'],
                                         extra_fan['icon_cate'],
                                         'https://m.panda.tv/type.html?cate=' +
                                         item['ename'],
                                         'https://www.panda.tv/cate/' +
                                         item['ename'],
                                         datetime.now(),
                                         item['ename'],
                                         self.platform_id))
                else:
                    # 插入操作
                    sql = '''insert into category(name, pic, icon, small_icon, mb_url, pc_url, short_name, platform_id, created_time) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['cname'],
                                         item['img'],
                                         extra_fan['icon'],
                                         extra_fan['icon_cate'],
                                         'https://m.panda.tv/type.html?cate=' +
                                         item['ename'],
                                         'https://www.panda.tv/cate/' +
                                         item['ename'],
                                         item['ename'],
                                         self.platform_id,
                                         datetime.now(),))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
