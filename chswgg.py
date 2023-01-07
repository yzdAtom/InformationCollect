"""
@Project ：InformationCollection 
@File    ：chswgg.py
@IDE     ：PyCharm 
@Author  ：Atomyzd
@Useage    ：长江航道局水位公告
"""
import time
import pymysql
import re
import requests
from lxml import etree
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0"
}

def get_detail_urls(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    ul = html.xpath("//div[@class='gl_list1']/ul//li//a/@href")
    base_url = "http://www.cjhdj.com.cn/hdfw/sw/"
    detail_urls = [base_url + i[2:] for i in ul]
    return detail_urls

def parse_detail_page(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    html_tables = html.xpath("//table[@border='1']")
    html_table = etree.tostring(html_tables[0], encoding="gbk").decode("gbk")
    swgg = pd.read_html(html_table)[0]
    swggnarray = np.array(swgg)
    swgglist = swggnarray.tolist()
    swgglist = swgglist[0]
    swgg.columns = swgglist
    swgg.drop([0], inplace=True)
    shijian = html.xpath("//div[@class='xl_tit1']/text()")[0]
    shijians = ["%s" % shijian] * swgg.shape[0]
    swgg["水位时间"] = pd.Series(shijians, index=range(1, swgg.shape[0]+1))
    return swgg

def main():
    db = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123456", database="cjhdtg")
    cursor = db.cursor()
    base_url = "http://www.cjhdj.com.cn/hdfw/sw/"
    detail_urls = get_detail_urls(base_url)
    for detail_url in detail_urls:
        print("当前正在抓取%s页面" % detail_url)
        swgg = parse_detail_page(detail_url)
        time.sleep(5)
        swgg.index = list(range(swgg.shape[0]))
        for j in range(swgg.shape[0]):
            zhandian = swgg.iloc[j, 0]
            shuiwei = swgg.iloc[j, 1]
            zhangluo = swgg.iloc[j, 2]
            shijian = swgg.iloc[j, 3]
            sql = "insert into swgg(zhandian, shuiwei, zhangluo, shijian) values(%s, %s, %s, %s)"
            cursor.execute(sql, (zhandian, shuiwei, zhangluo, shijian))
            db.commit()
    time.sleep(30)
    cursor.close()
    db.close()

if __name__ == '__main__':
    main()
