#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-15 15:11:24
# Project: zhanqi_anchor

from pyspider.libs.base_handler import *
import re
import pymysql
from datetime import datetime


class Handler(BaseHandler):
    headers = {
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
        self.limit = 20
        self.platform_id = 2
        try:
            self.connect = pymysql.connect(
                host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        page = 1

        try:
            cursor = self.connect.cursor()
            cursor.execute('select cate_id,id from category where platform_id = %s;' % str(self.platform_id))
            results = cursor.fetchall()
            for item in results:
                self.crawl('https://apis.zhanqi.tv/static/v2.1/game/live/%s/%s/%s.json?ver=3.0.1&os=3&platform=4&publisher=8&4G=0' %
                           (item[0], str(self.limit), str(page),),
                           callback=self.index_page,
                           save={
                               'page': page,
                               'cate_id': item[0],
                               'category_id': item[1],
                           })
        except Exception as e:
            self.connect.rollback()
            raise e

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        save = response.save
        if len(response.json['data']['rooms']) >= self.limit:
            save['page'] += 1
            self.crawl('https://apis.zhanqi.tv/static/v2.1/game/live/%s/%s/%s.json?ver=3.0.1&os=3&platform=4&publisher=8&4G=0' %
                       (save['cate_id'], str(self.limit), str(save['page']),),
                       callback=self.index_page,
                       save=save)

        for item in response.json['data']['rooms']:
            self.crawl('https://apis.zhanqi.tv/static/v2.1/room/%s.json?time=0.03288871&ver=3.0.1&os=3&platform=4&publisher=8&4G=0' %
                       (item['id']),
                       callback=self.detail_page,
                       save=save,
                       )

    @config(priority=2)
    def detail_page(self, response):
        return {
            "url": response.url,
            "results": response.json['data'],
            "cate_id": response.save['cate_id'],
            "category_id": response.save['category_id'],
        }

    def on_result(self,result):
        if not result:
            return
        self.save_data(**result)
        
    def save_data(self, **kw):

        if len(kw['results']) == 0:
            return

        if kw['results']:
            item = kw['results']
            try:
                cursor = self.connect.cursor()
                cursor.execute('select id from anchor where user_id=%s and platform_id=%s', (item['uid'], self.platform_id))
                result = cursor.fetchone()
                if result:
                    sql = '''update anchor set 
                        name=%s, 
                        room_id=%s, 
                        room_name=%s, 
                        cover=%s,
                        thumb_cover=%s,
                        avatar=%s,
                        avatar_mid=%s,
                        avatar_small=%s,
                        fans=%s,
                        category_id=%s,
                        cate_id=%s,
                        online=%s,
                        pc_url=%s,
                        announcement=%s,
                        update_time=%s,
                        show_time=%s  
                        where user_id=%s and platform_id=%s'''
                    cursor.execute(sql, (item['nickname'],  
                                         item['id'], 
                                         item['title'], 
                                         item['bpic'], 
                                         item['spic'],
                                         item['avatar'] + '-medium', 
                                         item['avatar'] + '-medium', 
                                         item['avatar'] + '-medium',  
                                         item['follows'],  
                                         kw['category_id'],  
                                         kw['cate_id'],  
                                         item['online'], 
                                         'https://www.zhanqi.tv/' + item['code'],
                                         ''.join(item['anchorNotice']),
                                         datetime.now(), 
                                         datetime.fromtimestamp(float(item['liveTime'])) if item['liveTime'] else datetime.now(),
                                         item['uid'], 
                                         self.platform_id))
                else:
                    # 插入操作
                    sql = '''insert into anchor(
                        user_id, 
                        name, 
                        room_id, 
                        room_name, 
                        cover, 
                        thumb_cover,
                        avatar, 
                        avatar_mid, 
                        avatar_small, 
                        fans, 
                        category_id, 
                        cate_id, 
                        online, 
                        platform_id, 
                        pc_url,
                        announcement,
                        show_time, 
                        created_time) 
                        values (%s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['uid'], 
                                         item['nickname'], 
                                         item['id'], 
                                         item['title'], 
                                         item['bpic'],
                                         item['spic'],
                                         item['avatar'] + '-medium', 
                                         item['avatar'] + '-medium', 
                                         item['avatar'] + '-medium', 
                                         item['follows'], 
                                         kw['category_id'], 
                                         kw['cate_id'], 
                                         item['online'], 
                                         self.platform_id,
                                         'https://www.zhanqi.tv/' + item['code'],
                                         ''.join(item['anchorNotice']),
                                         datetime.fromtimestamp(float(item['liveTime'])) if item['liveTime'] else datetime.now(),
                                         datetime.now(),
                                        ))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e