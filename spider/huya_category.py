#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-12-27 00:10:23
# Project: huya_category

from pyspider.libs.base_handler import *
import pymysql
from datetime import datetime
import time
import re
import json


class Handler(BaseHandler):
    crawl_config = {
        'headers': {
               'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1'
        }
    }
    
    def __init__(self):
        try:
            self.connect = pymysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
            
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e
        self.pattern_shortName = re.compile(r'.+?/(\w+)$')
        self.pattern_data = re.compile(r'.+?gameList = (.+);')

    @every(minutes=24 * 60)
    def on_start(self):
        # 网游竞技
        self.crawl('https://m.huya.com/g_ol', callback=self.detail_page)
        # 单机游戏
        time.sleep(3)
        self.crawl('https://m.huya.com/g_pc', callback=self.detail_page)
        # 手游
        time.sleep(3)
        self.crawl('https://m.huya.com/g_sy', callback=self.detail_page)
        # 娱乐
        time.sleep(3)
        self.crawl('https://m.huya.com/g_yl?areafib=1', callback=self.detail_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.attr.href, callback=self.detail_page)

    @config(priority=2)
    def detail_page(self, response):
        results = []
        # print(response.doc('script:last').text())
        # print(self.pattern_data.search(response.doc('script:last').text()).group(1))
        re_result = self.pattern_data.search(response.doc('script:last').text());
        dataListStr = re_result.group(1) if re_result else '[]'
        dataList = json.loads(dataListStr)
        for item in dataList:
            # shortName = self.pattern_shortName.match(item.find('a').attr('href'))
            
            result = {
                'name': item['gameFullName'],
                'short_name': item['gameHostName'],
                'pic': 'https://huyaimg.msstatic.com/cdnimage/game/%s-L.jpg' % item['gid'],
                'icon': None,
                'small_icon': None,
                'count': item['totalCount'],
                'mb_url': 'https://m.huya.com/g/' + item['gameHostName'],
                'pc_url': 'http://www.huya.com/g/' + item['gameHostName'],
                'platform_id': 4,
                
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
