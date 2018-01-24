#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-25 00:45:53
# Project: quanmin_anchor

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
        try:
            cursor = self.connect.cursor()
            cursor.execute('select short_name,id,cate_id from category where platform_id = %s;' % str(
                self.platform_id))
            results = cursor.fetchall()
            for item in results:
                self.crawl('https://www.quanmin.tv/json/categories/%s/list.json?01250041=&toid=0&token&sid&cv=xiaomi_3.5.33&ua=sagit&dev=28dc7f83c185d337&conn=WIFI&osversion=android_25&cid=6&nonce=b7560fbc6e56929469624ee3c9eb10f9&sign=658A5253C80A22054714887EC24CA693' %
                           (item[0],),
                           callback=self.detail_page,
                           save={
                               'short_name': item[0],
                               'category_id': item[1],
                               'cate_id': item[2],
                           })
        except Exception as e:
            self.connect.rollback()
            raise e

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        return {
            "url": response.url,
            "results": response.json['data'],
            "category_id": response.save['category_id'],
            "cate_id": response.save['cate_id'],
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
                cursor.execute('select id from anchor where user_id=%s and platform_id=%s', (item['uid'],self.platform_id))
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
                        announcement=%s,
                        beauty_cover=%s,
                        show_time=%s  
                        where user_id=%s and platform_id=%s'''
                    cursor.execute(sql, (item['nick'],  
                                         item['no'], 
                                         item['title'], 
                                         item['thumb'], 
                                         item['avatar'], 
                                         item['avatar'], 
                                         item['avatar'],  
                                         item['follow'],  
                                         kw['category_id'],  
                                         kw['cate_id'],  
                                         item['view'], 
                                         'https://www.quanmin.tv/' + item['no'],
                                         datetime.now(), 
                                         item['announcement'],
                                         item['beauty_cover'] if 'beauty_cover' in item else '',
                                         item['play_at'],
                                         item['uid'], 
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
                        announcement, 
                        beauty_cover, 
                        created_time) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['uid'], 
                                         item['nick'], 
                                         item['no'], 
                                         item['title'], 
                                         item['thumb'],
                                         item['avatar'], 
                                         item['avatar'], 
                                         item['avatar'], 
                                         item['follow'], 
                                         kw['category_id'], 
                                         kw['cate_id'], 
                                         item['view'], 
                                         self.platform_id,
                                         'https://www.quanmin.tv/' + item['no'],
                                         item['play_at'],
                                         item['announcement'],
                                         item['beauty_cover'] if 'beauty_cover' in item else '',
                                         datetime.now(),
                                        ))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
