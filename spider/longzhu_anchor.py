#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-16 17:24:00
# Project: longzhu_anchor

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
        self.pageSize = 30
        try:
            self.connect = pymysql.connect(
                host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        offset = 0

        try:
            cursor = self.connect.cursor()
            cursor.execute('select cate_id,id from category where platform_id = %s and cate_id not NULL;' % str(
                self.platform_id))
            results = cursor.fetchall()
            for item in results:
                self.crawl('https://stark.longzhu.com/api/v2/stream/search?type=7&target=%s&start-index=%s&max-results=%s&parentId=10105&version=4.6.2&device=4&packageId=1&utm_sr=chanel_10' %
                           (item[0], str(offset), str(self.pageSize),),
                           callback=self.detail_page,
                           save={
                               'pageSize': self.pageSize,
                               'offset': offset,
                               'cate_id': item[0],
                               'category_id': item[1],
                           })
        except Exception as e:
            self.connect.rollback()
            raise e


    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        save = response.save
        # 存储列表中的主播信息
        self.save_list_info(response.json['data']['streamFlows'], save)
        if len(response.json['data']['']) >= self.pageSize:
            save['offset'] += self.pageSize
            self.crawl('https://stark.longzhu.com/api/v2/stream/search?type=7&target=%s&start-index=%s&max-results=%s&parentId=10105&version=4.6.2&device=4&packageId=1&utm_sr=chanel_10' %
                           (save['cate_id'], str(save['offset']), str(self.pageSize),),
                           callback=self.index_page,
                           save=save)

        for result in response.json['data']['streamFlows']:
            for item in result['streams']:
                self.crawl('http://roomapicdn.longzhu.com/room/RoomAppStatusV2?roomId=%s&version=4.6.2&device=4&packageId=1&utm_sr=chanel_10' %
                           (item['room']['id'],),
                           callback=self.detail_page,
                           save=save)

    def save_list_info(self, results, save):
        if len(results) == 0:
            return
        for result in results:
            for item in result['streams']:
                try:
                    cursor = self.connect.corsor()
                    cursor.execute('select id from anchor where platform_id = %s and user_id = %s;' % (str(self.platform_id), item['user']['uid']))
                    sql_result = cursor.fetchone()
                    if sql_result:
                        # 更新
                        sql = '''
                        update anchor set 
                        name=%s, 
                        room_id=%s, 
                        room_name=%s, 
                        room_src=%s,
                        avatar=%s,
                        avatar_mid=%s,
                        avatar_small=%s,
                        category_id=%s,
                        cate_id=%s,
                        pc_url=%s,
                        update_time=%s,
                        show_time=%s 
                        where user_id=%s and platform_id=%s;
                        '''
                        cursor.execute(sql, (item['user']['name'],  
                                         item['room']['id'], 
                                         item['room']['title'], 
                                         item['cover'], 
                                         item['user']['avatar'], 
                                         item['user']['avatar'], 
                                         item['user']['avatar'],    
                                         save['category_id'],  
                                         save['cate_id'],  
                                         item['adverts']['ad_target'],
                                         datetime.now(), 
                                         datetime.fromtimestamp(float(item['room']['broadcast_begin'])) if item['room']['broadcast_begin'] else datetime.now(),
                                         item['user']['uid'], 
                                         self.platform_id))

                    else:
                        # 插入
                        sql = '''insert into anchor(
                        user_id, 
                        name, 
                        room_id, 
                        room_name, 
                        room_src, 
                        avatar, 
                        avatar_mid, 
                        avatar_small, 
                        category_id, 
                        cate_id, 
                        platform_id, 
                        pc_url,
                        show_time, 
                        created_time) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                        cursor.execute(sql, (item['user']['uid'], 
                                            item['user']['name'], 
                                            item['room']['id'], 
                                            item['room']['title'], 
                                            item['cover'],
                                            item['user']['avatar'], 
                                            item['user']['avatar'], 
                                            item['user']['avatar'], 
                                            save['category_id'], 
                                            save['cate_id'], 
                                            self.platform_id,
                                            item['adverts']['ad_target'],
                                            datetime.fromtimestamp(float(item['room']['broadcast_begin'])) if item['room']['broadcast_begin'] else datetime.now(),
                                            datetime.now(),
                                            ))

                    self.connect.commit()
                except Exception as e:
                    self.connect.rollback()
                    raise e


    @config(priority=2)
    def detail_page(self, response):
        save = response.save
        return {
            "url": response.url,
            "results": response.json['BaseRoomInfo'],
        }