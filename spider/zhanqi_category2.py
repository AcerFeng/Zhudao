#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-12-24 23:33:55
# Project: zhanqi_category

from pyspider.libs.base_handler import *
import pymysql
from datetime import datetime
import re

class Handler(BaseHandler):
    headers = {
        'Content-Length': '0',
        'Host': 'apis.zhanqi.tv',
        'Connection': 'Keep-Alive',
        'Cookie': 'PHPSESSID=5j91os2k90mc86gpjdl5t999t7',
        'Cookie2': '$Version=1',
        'Accept-Encoding': 'gzip',
        'User-Agent': 'Zhanqi.tv Api Client',
    }
    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }

    def __init__(self):
        self.platform_id = 2
        self.pattern_shortName = re.compile(r'games/(\w+)$')
        try:
            self.connect = pymysql.connect(
                host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')

        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://apis.zhanqi.tv/static/v2.2/game/lists/4/app.json?ver=3.0.1&os=3&platform=4&publisher=8&4G=0',
                   callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        for item in response.json['data']['games']:
            re_short_name = self.pattern_shortName.search(item['url'])
            print(re_short_name.group(1))

        return {
            "url": response.url,
            "title": response.doc('title').text(),
            "results": response.json['data']['games']
        }

    def on_result(self, result):
        if not result:
            return
        self.save_data(**result)

    def save_data(self, **kw):

        if len(kw['results']) == 0:
            return

        for item in kw['results']:
            re_short_name = self.pattern_shortName.search(item['url'])
            short_name = re_short_name.group(1) if re_short_name else ''
            try:
                cursor = self.connect.cursor()
                cursor.execute('select id from category where short_name=%s and platform_id=%s', (
                    short_name, self.platform_id))
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
                        update_time=%s,
                        cate_id=%s
                        where short_name=%s and platform_id=%s'''
                    cursor.execute(sql, (item['name'],
                                         item['spic'],
                                         item['appIcon'],
                                         item['tvIcon'],
                                         'https://m.zhanqi.tv/api/static/game.lives/' +
                                         item['id'] + '/',
                                         'https://www.zhanqi.tv' + item['url'],
                                         datetime.now(),
                                         item['id'],
                                         short_name,
                                         self.platform_id))
                else:
                    # 插入操作
                    sql = '''insert into category(name, pic, icon, small_icon, mb_url, pc_url, short_name, platform_id, created_time, cate_id) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['name'],
                                         item['spic'],
                                         item['appIcon'],
                                         item['tvIcon'],
                                         'https://m.zhanqi.tv/api/static/game.lives/' +
                                         item['id'] + '/',
                                         'https://www.zhanqi.tv' + item['url'],
                                         short_name,
                                         self.platform_id,
                                         datetime.now(),
                                         item['id'],
                                         ))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
