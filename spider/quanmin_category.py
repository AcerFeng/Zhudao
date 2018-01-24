#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-21 00:00:58
# Project: quanmin_category

from pyspider.libs.base_handler import *
import pymysql
from datetime import datetime

class Handler(BaseHandler):
    headers = {
        'Host': 'www.quanmin.tv',
        'Connection': 'Keep-Alive',
        'Accept-Encoding': 'gzip',
        'User-Agent': 'okhttp/3.9.1',
    }

    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }

    def __init__(self):
        self.platform_id = 6
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://www.quanmin.tv/json/app/index/category/info-android.json?01202349=&toid=0&token&sid&cv=xiaomi_3.5.33&ua=sagit&dev=28dc7f83c185d337&conn=WIFI&osversion=android_25&cid=6&nonce=9f1ca2d0ab4594e910dfbbe1ad2c0f97&sign=AAB46AC567D0A8E7B69443EA49B39B2A', callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        
        return {
            "url": response.url,
            "results": response.json,
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
                cursor.execute('select id from category where short_name=%s and platform_id=%s', (item['slug'], self.platform_id))
                result = cursor.fetchone()
                if result:
                    # 更新操作
                    sql = '''update category set 
                        icon=%s, 
                        small_icon=%s,
                        name=%s,
                        cate_id=%s,
                        update_time=%s
                        where short_name=%s and platform_id=%s'''
                    cursor.execute(sql, (item['icon_image'],
                                         item['icon_red'],  
                                         item['name'],
                                         item['id'], 
                                         datetime.now(),
                                         item['slug'], 
                                         self.platform_id))
                else:
                    # 插入操作
                    sql = '''insert into category(name, short_name, icon, small_icon, cate_id, platform_id, created_time) 
                    values (%s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['name'], 
                                         item['slug'], 
                                         item['icon_image'],
                                         item['icon_red'], 
                                         item['id'], 
                                         self.platform_id,
                                         datetime.now(),))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
