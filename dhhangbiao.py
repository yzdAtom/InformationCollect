"""
@Project ：InformationCollection 
@File    ：dhhangbiao.py
@IDE     ：PyCharm 
@Author  ：Atomyzd
@Useage    ：东海保障中心航标动态（标题，内容，来源， 时间， 附件，附件本地保存url）
"""
import time
import pymysql
import re
from urllib import request
import os
from bs4 import BeautifulSoup
import numpy as np
import threading
import requests
import pandas as pd
import urllib
from lxml import etree
import random
import ssl
from pdfminer.high_level import extract_text

ssl._create_default_https_context = ssl._create_unverified_context
headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0"
}

def get_detail_urls(url):
    resp = requests.get(url, headers=headers, verify =False)
    text = resp.content.decode('utf-8')
    # print(text)
    html = etree.HTML(text)
    ul = html.xpath("//ul[@class='list_n3']//li/a/@href")
    base_url = "https://www.dhhb.org.cn"
    detail_urls = [base_url + i for i in ul]
    # print(detail_urls)
    return detail_urls

def parse_detail_page(url):
    resp = requests.get(url, headers=headers, verify=False)
    text = resp.content.decode('utf-8')
    # print(text)
    html = etree.HTML(text)
    # 获取标题
    title = html.xpath("//div[@class='bt']/text()")[0]
    # 获取来源（东海保障中心）
    source = "东保中心"
    # 获取时间
    fabutime = html.xpath("//div[@class='sj']/text()")[0]
    mat = re.search(r"(\d{4}-\d{1,2}-\d{1,2})", fabutime)
    fabutime = mat.group(0)
    # 获取附件
    fujian_link = html.xpath("//div[@class='nr']/a/@href")[0]
    fujian_link = "https://www.dhhb.org.cn" + fujian_link
    filename = re.sub(r"[\/\\\:\*\?\"\<\>\|]", "", title)
    opener = urllib.request.build_opener()
    ua_list = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.62',
               'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0',
               'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.81 Safari/537.36 SE 2.X MetaSr 1.0'
               ]
    opener.addheaders = [('User-Agent', random.choice(ua_list))]
    urllib.request.install_opener(opener)
    try:
        urllib.request.urlretrieve(fujian_link, '../HBDT/%s.pdf' % title)
    except Exception as e:
        count = 1
        while count <= 9:
            time.sleep(3)
            print("尝试第%d次" % count)
            try:
                urllib.request.urlretrieve(fujian_link, '../HBDT/%s.pdf' % title)
                break
            except Exception as e:
                count += 1
        if count > 9:
            print("附件下载失败！！！")
    # 本地保存路径
    local_url = '../HBDT/%s.pdf' % title
    # 获取内容（东保中心的内容从pdf识别）
    # 首先判断文件是否下载成功
    if os.path.exists('../HBDT/%s.pdf' % title):
        hbcontent = extract_text('../HBDT/%s.pdf' % title)
        # print(text)
    else:
        hbcontent = title
    return title, source, fabutime, local_url, hbcontent


def main():
    db = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123456", database="cjhdtg")
    cursor = db.cursor()
    base_url = "https://www.dhhb.org.cn/index.aspx?cat_code=hbdtzl"
    # detail_urls = get_detail_urls(base_url)
    pages_list = list()
    for i in range(4, 11):
        last_item = "&page=%d" % i
        pages_list.append(last_item)
    for last_url in pages_list:
        url = base_url + last_url
        print("当前准备抓取:%s页面的内容" % url)
        detail_urls = get_detail_urls(url)
        for detail_url in detail_urls:
            print("当前正在抓取%s页面" % detail_url)
            try:
                title, source, fabutime, local_url, hbcontent = parse_detail_page(detail_url)
            except Exception as e:
                continue
            time.sleep(5)
            sql = "insert into hangbiaodongtai(title, source, time, local_url, hbcontent) values(%s, %s, %s, %s, %s)"
            cursor.execute(sql, (title, source, fabutime, local_url, hbcontent))
            db.commit()
        time.sleep(10)
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()
