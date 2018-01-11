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
    crawl_config = {
    }
    
    def __init__(self):
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://api.m.panda.tv/index.php?method=category.list&type=game', callback=self.detail_page)
        time.sleep(3)
        self.crawl('https://api.m.panda.tv/index.php?method=category.list&type=yl&__plat=ios', callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        results = []
        for item in response.json['data']:
            extra_fan = json.loads(item['extra'])
            result = {
                'name': item['cname'],
                'short_name': item['ename'],
                'pic': item['img'],
                'icon': extra_fan['icon'],
                'small_icon': extra_fan['icon_cate'],
                'count': 0,
                'mb_url': 'https://m.panda.tv/type.html?cate=' + item['ename'],
                'pc_url': 'https://www.panda.tv/cate/' + item['ename'],
                'platform_id': 3,
                
            }
            results.append(result)
        
        return {
            "url": response.url,
            "title": response.doc('title').text(),
            "results": results
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
                cursor.execute('select count(*) from category where short_name=%s', (item['short_name'],))
                result = cursor.fetchone()
                if result[0]:
                    # 更新操作
                    sql = '''update category set 
                        name=%s, 
                        pic=%s, 
                        icon=%s, 
                        small_icon=%s,
                        count=%s,
                        mb_url=%s,
                        pc_url=%s,
                        update_time=%s
                        where short_name=%s and platform_id=%s'''
                    cursor.execute(sql, (item['name'], 
                                         item['pic'], 
                                         item['icon'], 
                                         item['small_icon'], 
                                         item['count'], 
                                         item['mb_url'], 
                                         item['pc_url'], 
                                         datetime.now(),
                                         item['short_name'], 
                                         item['platform_id']))
                else:
                    # 插入操作
                    sql = '''insert into category(name, pic, icon, small_icon, count, mb_url, pc_url, short_name, platform_id, created_time) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['name'], 
                                         item['pic'], 
                                         item['icon'], 
                                         item['small_icon'], 
                                         item['count'],
                                         item['mb_url'], 
                                         item['pc_url'], 
                                         item['short_name'], 
                                         item['platform_id'],
                                        datetime.now(),))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
