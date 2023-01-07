"""
@Project ：InformationCollection 
@File    ：hsjhxjg.py
@IDE     ：PyCharm 
@Author  ：Atomyzd
@Useage    ：各海事局航行警告抓取
"""
import time
import pymysql
import re
from bs4 import BeautifulSoup
import numpy as np
import threading
import requests
import pandas as pd
from lxml import etree


headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0"
}

def parse_detail_page(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    # 获取标题
    title = html.xpath("//h2[@id='title']/text()")[0]
    # print(title)
    # 获取发布时间
    time_text = html.xpath("//div[@class='pull-left']/span[3]/text()")[0]
    mat = re.search(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2})", time_text)
    time_text = mat.group(0)
    # print(time_text)
    # 获取内容
    ggcontent = html.xpath("//div[@id='ch_p']/p/text()")
    ggcontent = "\n".join(ggcontent)
    # print(ggcontent)
    # 获取来源
    laiyuan = html.xpath("//div[@class='pull-left']/span[1]/text()")[0]
    laiyuan = laiyuan[3:]
    return title, time_text, ggcontent, laiyuan


def main():
    db = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123456", database="cjhdtg")
    cursor = db.cursor()
    base_url = "https://www.msa.gov.cn/page/outter/weather.jsp"
    resp = requests.get(base_url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    hxjg_list = html.xpath("//ul[@class='left_nav_wrap']/li[2]/ul/li/@cid")
    hxtg_list = html.xpath("//ul[@class='left_nav_wrap']/li[3]/ul/li/@cid")
    # all_list = hxjg_list + hxtg_list
    for i in hxtg_list:
        print(i)
        pageNo = 1
        while True:
            print(pageNo)
            if pageNo == 10:
                break
            request_url = "https://www.msa.gov.cn/page/openInfo/articleList.do?channelId=%s&pageSize=20&pageNo=%d&isParent=0" % (i, pageNo)
            print(request_url)
            resp = requests.get(request_url, headers=headers)
            time.sleep(10)
            text = resp.content.decode('utf-8')
            html = etree.HTML(text)
            ultemp = html.xpath("//ul[@class='main_list_ul active']//li")
            if ultemp == []:
                break
            else:
                article_list = html.xpath("//ul[@class='main_list_ul active']//li/a/@href")
                for article_link in article_list:
                    link_url = "https://www.msa.gov.cn" + article_link
                    print(link_url)
                    try:
                        title, time_text, ggcontent, laiyuan = parse_detail_page(link_url)
                    except Exception as e:
                        continue
                    time.sleep(2)
                    if i in set(hxjg_list):
                        cate = "航行警告"
                    elif i in set(hxtg_list):
                        cate = "航行通告"
                    sql = "insert into hdtgjg(cate, title, time, content, source) values(%s, %s, %s, %s, %s)"
                    cursor.execute(sql, (cate, title, time_text, ggcontent, laiyuan))
                    db.commit()
                pageNo += 1
    cursor.close()
    db.close()


if __name__ == '__main__':
    main()

