"""
@Project ：InformationCollection 
@File    ：cjqxyg.py
@IDE     ：PyCharm 
@Author  ：Atomyzd
@Useage    ：长江航务管理局气象预告
"""
import time
import pymysql
import re
from bs4 import BeautifulSoup
import numpy as np
import requests
import pandas as pd
from lxml import etree

headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0"
}

def get_detail_urls(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    # print(text)
    html = etree.HTML(text)
    ul = html.xpath("//div[@class='newslist']/ul//li/a/@href")
    # print(ul)
    base_url = "https://cjhy.mot.gov.cn/zxfw/cxfw/qxyg/"
    detail_urls = [base_url + i[2:] for i in ul]
    return detail_urls


def parse_detail_page(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    # print(text)
    # 获取表格
    html_tables = html.xpath("//table[@border='1']")
    html_table = etree.tostring(html_tables[0], encoding="gbk").decode("gbk")
    qiwen_table = pd.read_html(html_table)[0]
    qiwenarray = np.array(qiwen_table)
    qiwenlist = qiwenarray.tolist()
    qiwenlist = qiwenlist[0]
    qiwen_table.columns = qiwenlist
    qiwen_table.drop([0], inplace=True)
    # 新添加一列，预报时间
    yubao_time = etree.tostring(html.xpath("//table[@border='1']/preceding-sibling::*[1]")[0], encoding="gbk").decode("gbk")
    soup = BeautifulSoup(yubao_time, 'lxml')
    yubao_time = soup.span.get_text()[5:]
    qiwentime = ["%s" % yubao_time] * qiwen_table.shape[0]
    qiwen_table["预报时间"] = pd.Series(qiwentime, index=range(1, qiwen_table.shape[0]+1))
    return qiwen_table



def main():
    db = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123456", database="cjhdtg")
    cursor = db.cursor()
    base_url = "https://cjhy.mot.gov.cn/zxfw/cxfw/qxyg/"
    pages_list = ["index.shtml"]
    for i in range(1, 67):
        last_item = "index_%d.shtml" % i
        pages_list.append(last_item)
    for last_url in pages_list:
        url = base_url + last_url
        print("当前准备抓取:%s页面的内容" % url)
        detail_urls = get_detail_urls(url)
        for detail_url in detail_urls:
            print("当前正在抓取%s页面" % detail_url)
            try:
                qiwentable = parse_detail_page(detail_url)
            except Exception as e:
                continue
            time.sleep(5)
            qiwentable.index = list(range(qiwentable.shape[0]))
            # 将csv中的内容逐一读入数据库
            try:
                for j in range(qiwentable.shape[0]):
                    zhandian = qiwentable.iloc[j, 0]
                    tianqi = qiwentable.iloc[j, 1]
                    zuidiqiwen = qiwentable.iloc[j, 2]
                    zuigaoqiwen = qiwentable.iloc[j, 3]
                    fengxiang = qiwentable.iloc[j, 4]
                    dinenejiandu = qiwentable.iloc[j, 5]
                    gaonengjiandu = qiwentable.iloc[j, 6]
                    wu = qiwentable.iloc[j, 7]
                    mai = qiwentable.iloc[j, 8]
                    yubaoshijian = qiwentable.iloc[j, 9]
                    sql = "insert into cjqxyb(zhandian, tianqi, zuidiqiwen, zuigaoqiwen, fengxiang, dinenejiandu, gaonengjiandu, wu, mai, yubaoshijian) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (zhandian, tianqi, zuidiqiwen, zuigaoqiwen, fengxiang, dinenejiandu, gaonengjiandu, wu, mai, yubaoshijian))
                    db.commit()
            except Exception as e:
                continue
        time.sleep(30)
    cursor.close()
    db.close()


if __name__ == '__main__':
    main()
