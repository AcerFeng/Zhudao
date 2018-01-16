#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-15 20:44:02
# Project: huya_anchor

from pyspider.libs.base_handler import *
import re
import pymysql
from datetime import datetime


class Handler(BaseHandler):
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cookie': 'Hm_lvt_51700b6c722f5bb4cf39906a596ea41f=1516011164; udb_passdata=1; SoundValue=0.50; __yasmid=0.5270737031266626; __yamid_tt1=0.5270737031266626; __yamid_new=C7D6261457500001F7ADA400A3E03CB0; _yasids=__rootsid%3DC7D626145900000180B56FAEA088A050; guid=0e74abb685985c5a075280808180b30c; ya_eid=index/jdt/1; isInLiveRoom=true; Hm_lpvt_51700b6c722f5bb4cf39906a596ea41f=1516017891; Hm_lvt_3a022a5f11ac1cfb68c9bbffeb894709=1516018170; Hm_lpvt_3a022a5f11ac1cfb68c9bbffeb894709=1516019587',
        'referer': 'https://m.huya.com/g/worldoftanks',
        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1',
        'x-requested-with': 'XMLHttpRequest',
    }

    crawl_config = {
        'itag': 'v001',
        'headers': headers,
    }

    def __init__(self):
        self.limit = 120
        self.platform_id = 4
        try:
            self.connect = pymysql.connect(
                host='localhost', port=3306, user='root', passwd='123456', db='zhudao', charset='utf8mb4')
        except Exception as e:
            print('Cannot Connect To Mysql!/n', e)
            raise e

    @every(minutes=24 * 60)
    def on_start(self):
        page = 1
        # http://screenshot.msstatic.com/yysnapshot/1801ef72a78b8c1d5ad03b646551b3d49344e5bf6aee?imageview/4/0/w/220/h/124/blur/1
        try:
            cursor = self.connect.cursor()
            cursor.execute('select cate_id,id from category where platform_id = %s;' % str(
                self.platform_id))
            results = cursor.fetchall()
            for item in results:
                self.crawl('https://m.huya.com/cache.php?m=Game&do=ajaxGetGameLive&gameId=%s&page=%s&pageSize=%s' %
                           (item[0], str(page), str(self.limit),),
                           callback=self.detail_page,
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

        if len(response.json['profileList']) >= self.limit:
            save['page'] += 1
            self.crawl('https://m.huya.com/cache.php?m=Game&do=ajaxGetGameLive&gameId=%s&page=%s&pageSize=%s' %
                           (item[0], str(page), str(self.limit),),
                           callback=self.index_page,
                           save={
                               'page': page,
                               'cate_id': item[0],
                               'category_id': item[1],
                           })

        for item in response.json['data']['items']:
            self.crawl('http://api.m.panda.tv/ajax_get_liveroom_baseinfo?slaveflag=1&type=json&roomid=%s&inroom=1&__plat=android&__version=3.3.0.5930&__channel=xiaomi' %
                       (item['id']),
                       callback=self.detail_page,
                       save=save)

    @config(priority=2)
    def detail_page(self, response):
        save = response.save

        if len(response.json['profileList']) >= self.limit:
            save['page'] += 1
            self.crawl('https://m.huya.com/cache.php?m=Game&do=ajaxGetGameLive&gameId=%s&page=%s&pageSize=%s' %
                           (save['cate_id'], str(save['page']), str(self.limit),),
                           callback=self.index_page,
                           save=save)
            
        for item in response.json['profileList']:
            self.crawl('http://api.huya.com/subscribe/getSubscribeStatus?from_key=&from_type=1&to_key=%s&to_type=2&callback=jQuery111108152252697200741_1516068669069&_=1516068669070' %
                           (item['uid'],),
                           callback=self.get_subscribe,
                           save=save)
        return {
            "url": response.url,
            "results": response.json['profileList'],
        }
    
    def get_subscribe(self, response):
        save = response.save
        print(save)
        print(response.doc('*').text())
