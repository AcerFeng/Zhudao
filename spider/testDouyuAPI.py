# -*- encoding: utf-8 -*-

import hashlib
import requests
from datetime import datetime

if __name__ == '__main__':
    # http://capi.douyucdn.cn/api/v1/room/<room_id>?aid=android&client_sys=android&time=<time>&auth=c0a6170a754ca187e8a52a3343ecf273
    # r = requests.get('https://www.douyu.com/gapi/rkc/directory/2_124/1')

    room_id = '105025'
    time = str(int(datetime.now().timestamp() / 1000))
    md = hashlib.md5()
    hash_str = "room/"+room_id+"?aid=android&client_sys=android&time="+ time
    md.update(hash_str.encode('utf-8'))
    auth = md.hexdigest()
    url = 'http://capi.douyucdn.cn/api/v1/room/%s?aid=android&client_sys=android&time=%s&auth=%s' % (room_id, time, auth)
    print(url)


    # print(r.text)