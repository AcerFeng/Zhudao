#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-12-24 23:33:55
# Project: zhanqi_category

from pyspider.libs.base_handler import *
import pymysql
from datetime import datetime


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
        self.crawl('https://m.zhanqi.tv/api/static/game.lists/100-1.json', callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        results = []
        for item in response.json['data']['games']:
            result = {
                'name': item['name'],
                'short_name': item['gameKey'],
                'pic': item['spic'],
                'icon': item['appIcon'],
                'small_icon': item['tvIcon'],
                'count': 0,
                'mb_url': 'https://m.zhanqi.tv/api/static/game.lives/' + item['id'] + '/',
                'pc_url': 'https://www.zhanqi.tv' + item['url'],
                'platform_id': 2,
                'cate_id': item['id'],
                
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
                cursor.execute('select id from category where short_name=%s and platform_id=%s', (item['short_name'],item['platform_id']))
                result = cursor.fetchone()
                if result:
                    # 更新操作
                    sql = '''update category set 
                        name=%s, 
                        pic=%s, 
                        icon=%s, 
                        small_icon=%s,
                        count=%s,
                        mb_url=%s,
                        pc_url=%s,
                        update_time=%s,
                        cate_id=%s
                        where short_name=%s and platform_id=%s'''
                    cursor.execute(sql, (item['name'], 
                                         item['pic'], 
                                         item['icon'], 
                                         item['small_icon'], 
                                         item['count'], 
                                         item['mb_url'], 
                                         item['pc_url'], 
                                         datetime.now(),
                                         item['cate_id'],
                                         item['short_name'], 
                                         item['platform_id']))
                else:
                    # 插入操作
                    sql = '''insert into category(name, pic, icon, small_icon, count, mb_url, pc_url, short_name, platform_id, created_time, cate_id) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['name'], 
                                         item['pic'], 
                                         item['icon'], 
                                         item['small_icon'], 
                                         item['count'],
                                         item['mb_url'], 
                                         item['pc_url'], 
                                         item['short_name'], 
                                         item['platform_id'],
                                        datetime.now(),
                                         item['cate_id'],
                                        ))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
