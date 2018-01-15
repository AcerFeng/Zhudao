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
        'itag': 'v001',
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
            "title": response.json['data']['info'],
        }
