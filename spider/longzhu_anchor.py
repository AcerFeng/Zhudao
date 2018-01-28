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

    headers2 = {
        'Host': 'roomapicdn.longzhu.com',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'UM_distinctid=16064da9e0a283-04a3e3f63ff7f1-163c6657-384000-16064da9e0b1ce; _ma=OREN.2.1237551537.1513520538; pluguest=1C7B2F78153E8362A4E030C381473D996CCC5F4C9EED1377AED4E4AF22E9EC4664EB8F515F1B0AF817B1D9D4808744FFCA899CC53310F667; __mtmc=2.313689436.1515691819; p1u_id=-1; __mtmb=2.723959108.1516115307',
        'If-Modified-Since': 'Tue, 16 Jan 2018 15:45:20 GMT'
    }

    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }

    def __init__(self):
        self.platform_id = 5
        self.pageSize = 30
        self.show_time_par = re.compile(r'\((\d+)\+')
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
            cursor.execute('select cate_id,id from category where platform_id = %s and cate_id is not null;' % str(
                self.platform_id))
            results = cursor.fetchall()
            for item in results:
                self.crawl('https://stark.longzhu.com/api/v2/stream/search?type=7&target=%s&start-index=%s&max-results=%s&parentId=10105&version=4.6.2&device=4&packageId=1&utm_sr=chanel_10' %
                           (item[0], str(offset), str(self.pageSize),),
                           callback=self.index_page,
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
        if len(response.json['data']['streamFlows']) >= 1 and len(response.json['data']['streamFlows'][0]['streams']) >= self.pageSize:
            save['offset'] += self.pageSize
            self.crawl('https://stark.longzhu.com/api/v2/stream/search?type=7&target=%s&start-index=%s&max-results=%s&parentId=10105&version=4.6.2&device=4&packageId=1&utm_sr=chanel_10' %
                       (save['cate_id'], str(save['offset']),
                        str(self.pageSize),),
                       callback=self.index_page,
                       save=save)

        for result in response.json['data']['streamFlows']:
            for item in result['streams']:
                self.crawl('http://roomapicdn.longzhu.com/room/RoomAppStatusV2?roomId=%s&version=4.6.2&device=4&packageId=1&utm_sr=chanel_10' %
                           (item['room']['id'],),
                           callback=self.detail_page,
                           save=save,
                           headers=Handler.headers2,
                           )

    @config(priority=2)
    def detail_page(self, response):
        return {
            "url": response.url,
            "result": response.json,
            "save": response.save,
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
                            (item['BaseRoomInfo']['UserId'], self.platform_id))
            result = cursor.fetchone()
            re_show_time = self.show_time_par.search(item['Broadcast']['BeginTime'])
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
                    cate_id=%s,
                    online=%s,
                    pc_url=%s,
                    `desc`=%s,
                    update_time=%s,
                    show_time=%s
                    where id=%s'''
                cursor.execute(sql, (item['BaseRoomInfo']['Name'],
                                        item['BaseRoomInfo']['Id'],
                                        item['BaseRoomInfo']['BoardCastTitle'],
                                        item['RoomScreenshot'],
                                        item['BaseRoomInfo']['Avatar'],
                                        item['BaseRoomInfo']['Avatar'],
                                        item['BaseRoomInfo']['Avatar'],
                                        item['BaseRoomInfo']['SubscribeCount'],
                                        save['category_id'],
                                        save['cate_id'],
                                        item['OnlineCount'],
                                        'http://star.longzhu.com/' + item['BaseRoomInfo']['Domain'],
                                        item['BaseRoomInfo']['Desc'],
                                        datetime.now(),
                                        datetime.fromtimestamp(float(re_show_time.group(1))/1000) if re_show_time else None,
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
                    cate_id,
                    online,
                    platform_id,
                    pc_url,
                    `desc`,
                    show_time,
                    created_time)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                cursor.execute(sql, (item['BaseRoomInfo']['UserId'],
                                        item['BaseRoomInfo']['Name'],
                                        item['BaseRoomInfo']['Id'],
                                        item['BaseRoomInfo']['BoardCastTitle'],
                                        item['RoomScreenshot'],
                                        item['BaseRoomInfo']['Avatar'],
                                        item['BaseRoomInfo']['Avatar'],
                                        item['BaseRoomInfo']['Avatar'],
                                        item['BaseRoomInfo']['SubscribeCount'],
                                        save['category_id'],
                                        save['cate_id'],
                                        item['OnlineCount'],
                                        self.platform_id,
                                        'http://star.longzhu.com/' + item['BaseRoomInfo']['Domain'],
                                        item['BaseRoomInfo']['Desc'],
                                        datetime.fromtimestamp(float(re_show_time.group(1))/1000) if re_show_time else None,
                                        datetime.now(),
                                    ))
            self.connect.commit()

        except Exception as e:
            self.connect.rollback()
            raise e

