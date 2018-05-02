#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-15 16:23:24
# Project: xiongmao_anchor

from pyspider.libs.base_handler import *
import re
import pymysql
from datetime import datetime


class Handler(BaseHandler):
    headers = {
        'Host': 'api.m.panda.tv',
        'Connection': 'Keep-Alive',
        'Cookie': 'pdftv1=7c5fa|160f8d41911|77eb|b70e8684|e',
        'Accept-Encoding': 'gzip',
        'User-Agent': 'okhttp/3.9.0',
    }

    crawl_config = {
        'itag': 'v002',
        'headers': headers,
    }

    def __init__(self):
        self.limit = 20
        self.platform_id = 3
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
            cursor.execute('select short_name,id from category where platform_id = %s;' % str(self.platform_id))
            results = cursor.fetchall()
            for item in results:
                self.crawl('http://api.m.panda.tv/ajax_get_live_list_by_cate?cate=%s&pageno=%s&pagenum=%s&sproom=1&banner=1&slider=1&__version=3.3.0.5930&__plat=android&__channel=xiaomi' %
                           (item[0], str(page), str(self.limit),),
                           callback=self.index_page,
                           save={
                               'page': page,
                               'short_name': item[0],
                               'category_id': item[1],
                           })
        except Exception as e:
            self.connect.rollback()
            raise e

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        save = response.save

        if len(response.json['data']['items']) >= self.limit:
            save['page'] += 1
            self.crawl('http://api.m.panda.tv/ajax_get_live_list_by_cate?cate=%s&pageno=%s&pagenum=%s&sproom=1&banner=1&slider=1&__version=3.3.0.5930&__plat=android&__channel=xiaomi' %
                           (save['short_name'], str(save['page']), str(self.limit),),
                           callback=self.index_page,
                           save=save)

        for item in response.json['data']['items']:
            self.crawl('http://api.m.panda.tv/ajax_get_liveroom_baseinfo?slaveflag=1&type=json&roomid=%s&inroom=1&__plat=android&__version=3.3.0.5930&__channel=xiaomi' %
                           (item['id']),
                           callback=self.detail_page,
                           save=save)

    @config(priority=2)
    def detail_page(self, response):
        return {
            "url": response.url,
            "result": response.json['data']['info'],
            "save": response.save
        }
    
    def on_result(self, result):
        if not result:
            return
        self.save_data(**result)
    
    def save_data(self, **kw):

        if not kw['result']:
            return

        item = kw['result']
        save = kw['save']

        try:
            cursor = self.connect.cursor()
            cursor.execute('select id from anchor where user_id=%s and platform_id=%s',
                            (item['hostinfo']['rid'], self.platform_id))
            result = cursor.fetchone()
            if result:
                # 更新操作(是否创建个主播分析表（新爬虫？）：包含平台、主播id、)
                sql = '''update anchor set
                    name=%s,
                    room_id=%s,
                    room_name=%s,
                    cover=%s,
                    avatar=%s,
                    avatar_mid=%s,
                    avatar_small=%s,
                    fans=%s,
                    category_id=%s,
                    online=%s,
                    pc_url=%s,
                    `desc`=%s,
                    update_time=%s,
                    show_time=%s
                    where id=%s'''
                cursor.execute(sql, (item['hostinfo']['name'],
                                        item['roominfo']['id'],
                                        item['roominfo']['name'],
                                        item['roominfo']['pictures']['img'],
                                        item['hostinfo']['avatar'],
                                        item['hostinfo']['avatar'],
                                        item['hostinfo']['avatar'],
                                        item['roominfo']['fans'],
                                        save['category_id'],
                                        item['roominfo']['person_num'],
                                        'https://www.panda.tv/' + item['roominfo']['id'],
                                        item['roominfo']['bulletin'],
                                        datetime.now(),
                                        datetime.fromtimestamp(float(item['roominfo']['start_time'])),
                                        result[0],))
            else:
                # 插入操作
                sql='''insert into anchor(
                    user_id,
                    name,
                    room_id,
                    room_name,
                    cover,
                    avatar,
                    avatar_mid,
                    avatar_small,
                    fans,
                    category_id,
                    online,
                    platform_id,
                    pc_url,
                    `desc`,
                    show_time,
                    created_time)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                cursor.execute(sql, (item['hostinfo']['rid'],
                                        item['hostinfo']['name'],
                                        item['roominfo']['id'],
                                        item['roominfo']['name'],
                                        item['roominfo']['pictures']['img'],
                                        item['hostinfo']['avatar'],
                                        item['hostinfo']['avatar'],
                                        item['hostinfo']['avatar'],
                                        item['roominfo']['fans'],
                                        save['category_id'],
                                        item['roominfo']['person_num'],
                                        self.platform_id,
                                        'https://www.panda.tv/' + item['roominfo']['id'],
                                        item['roominfo']['bulletin'],
                                        datetime.fromtimestamp(float(item['roominfo']['start_time'])),
                                        datetime.now(),
                                    ))
            self.connect.commit()

        except Exception as e:
            self.connect.rollback()
            raise e
