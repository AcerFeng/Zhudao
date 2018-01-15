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
            cursor.execute('select cate_id,id from category where platform_id = 2;')
            results = cursor.fetchall()
            for item in results:
                print(item)
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
        }