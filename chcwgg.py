"""
@Project ：InformationCollection 
@File    ：chcwgg.py
@IDE     ：PyCharm 
@Author  ：Atomyzd
@Useage    ：长江航道局潮位公告
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
    base_url = "http://www.cjhdj.com.cn/hdfw/cw/"
    detail_urls = [base_url + i[2:] for i in ul]
    return detail_urls

def parse_detail_page(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    html_tables = html.xpath("//table[@border='1']")
    html_table = etree.tostring(html_tables[0], encoding="gbk").decode("gbk")
    cwgg = pd.read_html(html_table)[0]
    cwggnarray = np.array(cwgg)
    cwgglist = cwggnarray.tolist()
    cwgglist = cwgglist[0]
    cwgg.columns = cwgglist
    cwgg.drop([0], inplace=True)
    return cwgg


def main():
    db = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123456", database="cjhdtg")
    cursor = db.cursor()
    base_url = "http://www.cjhdj.com.cn/hdfw/cw/"
    detail_urls = get_detail_urls(base_url)
    for detail_url in detail_urls:
        print("当前正在抓取%s页面" % detail_url)
        cwgg = parse_detail_page(detail_url)
        time.sleep(5)
        cwgg.index = list(range(cwgg.shape[0]))
        for j in range(cwgg.shape[0]):
            chaoweidian = cwgg.iloc[j, 0]
            chaowei = cwgg.iloc[j, 1]
            chaogao1 = cwgg.iloc[j, 2]
            if pd.isna(chaogao1):
                chaogao1 = "-"
            chaoshi1 = cwgg.iloc[j, 3]
            if pd.isna(chaoshi1):
                chaoshi1 = "0000-00-00 00:00:00"
            chaogao2 = cwgg.iloc[j, 4]
            if pd.isna(chaogao2):
                chaogao2 = "-"
            chaoshi2 = cwgg.iloc[j, 5]
            if pd.isna(chaoshi2):
                chaoshi2 = "0000-00-00 00:00:00"
            chaogao3 = cwgg.iloc[j, 6]
            if pd.isna(chaogao3):
                chaogao3 = "-"
            chaoshi3 = cwgg.iloc[j, 7]
            if pd.isna(chaoshi3):
                chaoshi3 = "0000-00-00 00:00:00"
            chaogao4 = cwgg.iloc[j, 8]
            if pd.isna(chaogao4):
                chaogao4 = "-"
            chaoshi4 = cwgg.iloc[j, 9]
            if pd.isna(chaoshi4):
                chaoshi4 = "0000-00-00 00:00:00"
            sql = "insert into cwgg(chaoweidian, chaowei, chaogao1, chaoshi1, chaogao2, chaoshi2, chaogao3, chaoshi3, chaogao4, chaoshi4) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (chaoweidian, chaowei, chaogao1, chaoshi1, chaogao2, chaoshi2, chaogao3, chaoshi3, chaogao4, chaoshi4))
            db.commit()
    time.sleep(30)
    cursor.close()
    db.close()


if __name__ == '__main__':
    main()
