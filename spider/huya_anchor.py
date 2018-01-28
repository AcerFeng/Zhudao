#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-01-15 20:44:02
# Project: huya_anchor

from pyspider.libs.base_handler import *
import re
import pymysql
from datetime import datetime
import json


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
        self.subscribe_par = re.compile(r'\((.+?)\)')
        self.avater_par = re.compile(r'\((.+)')
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
                           save={'uid': item['uid']})
            self.crawl('https://search.cdn.huya.com/?m=Search&do=getSearchContent&plt=m&q=%s&uid=0&app=11&v=1&typ=-5&start=0&rows=4&callback=jQuery200049810552581192247_1517068451880&_=1517068451882' %
                           (item['nick'],),
                           callback=self.get_avatar,
                           save={'uid': item['uid']})
        return {
            "url": response.url,
            "results": response.json['profileList'],
            "cate_id": response.save['cate_id'],
            "category_id": response.save['category_id'],
        }

    def get_avatar(self, response):
        re_avatar = self.avater_par.search(response.doc('*').text())
        if re_avatar:
            self.save_avatar(json.loads(re_avatar.group(1)), save=response.save)

    def save_avatar(self, result, save):
        if (not result.get('response')) or (not result.get('response').get('1')) or (not result.get('response').get('1').get('docs')):
            return
        docs = result.get('response').get('1').get('docs')
        for item in docs:
            if str(item['uid']) == str(save['uid']):
                print('if')
                try:
                    cursor = self.connect.cursor()
                    cursor.execute('select id from anchor where user_id=%s and platform_id=%s', (save['uid'], self.platform_id))
                    query_result = cursor.fetchone()
                    if query_result:
                        # 更新操作(是否创建个主播分析表（新爬虫？）：包含平台、主播id、)
                        sql = '''update anchor set 
                            avatar=%s,
                            avatar_mid=%s,
                            avatar_small=%s 
                            where user_id=%s and platform_id=%s'''
                        cursor.execute(sql, (item['game_avatarUrl180'],  
                                            item['game_avatarUrl180'],
                                            item['game_avatarUrl180'],
                                            save['uid'], 
                                            self.platform_id))
                    else:
                        # 插入操作
                        sql = '''insert into anchor(
                            user_id, 
                            avatar,  
                            avatar_mid,  
                            avatar_small, 
                            platform_id, 
                            created_time) 
                            values (%s, %s, %s, %s, %s, %s)'''
                        cursor.execute(sql, (save['uid'], 
                                            item['game_avatarUrl180'], 
                                            item['game_avatarUrl180'], 
                                            item['game_avatarUrl180'], 
                                            self.platform_id,
                                            datetime.now(),
                                            ))
                    self.connect.commit()

                except Exception as e:
                    self.connect.rollback()
                    raise e

                break
    
    def get_subscribe(self, response):
        re_sub = self.subscribe_par.search(response.doc('*').text())
        if re_sub:
            self.save_subscribe(json.loads(re_sub.group(1)), save=response.save)
        
    def save_subscribe(self, result, save):
        if result and result.get('subscribe_count'):
            try:
                cursor = self.connect.cursor()
                cursor.execute('select id from anchor where user_id=%s and platform_id=%s', (save['uid'], self.platform_id))
                query_result = cursor.fetchone()
                if query_result:
                    # 更新操作(是否创建个主播分析表（新爬虫？）：包含平台、主播id、)
                    sql = '''update anchor set 
                        fans=%s 
                        where user_id=%s and platform_id=%s'''
                    cursor.execute(sql, (result['subscribe_count'],  
                                         save['uid'], 
                                         self.platform_id))
                else:
                    # 插入操作
                    sql = '''insert into anchor(
                        user_id, 
                        fans, 
                        platform_id, 
                        created_time) 
                        values (%s, %s, %s, %s)'''
                    cursor.execute(sql, (save['uid'], 
                                         result['subscribe_count'], 
                                         self.platform_id,
                                         datetime.now(),
                                        ))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e
            


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
                        cover=%s,
                        category_id=%s,
                        cate_id=%s,
                        online=%s,
                        pc_url=%s,
                        update_time=%s
                        where user_id=%s and platform_id=%s'''
                    cursor.execute(sql, (item['nick'],  
                                         item['privateHost'], 
                                         item['introduction'], 
                                         item['screenshot'], 
                                         kw['category_id'],  
                                         kw['cate_id'],  
                                         item['totalCount'], 
                                         'http://www.huya.com/' + item['privateHost'],
                                         datetime.now(), 
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
                        category_id, 
                        cate_id, 
                        online, 
                        platform_id, 
                        pc_url,
                        created_time) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                    cursor.execute(sql, (item['uid'], 
                                         item['nick'], 
                                         item['privateHost'], 
                                         item['introduction'], 
                                         item['screenshot'],
                                         kw['category_id'], 
                                         kw['cate_id'], 
                                         item['totalCount'], 
                                         self.platform_id,
                                         'http://www.huya.com/' + item['privateHost'],
                                         datetime.now(),
                                        ))
                self.connect.commit()

            except Exception as e:
                self.connect.rollback()
                raise e

