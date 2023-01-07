"""
@Project ：InformationCollection 
@File    ：cjhdtgsy.py
@IDE     ：PyCharm 
@Author  ：Atomyzd
@Useage    ：长江航道局上游航道通告抓取
"""
import time
import pymysql
import re
import requests
from lxml import etree
from bs4 import BeautifulSoup

headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0"
}

# 获取详情列表页面
def get_detail_urls(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    # print(text)
    html = etree.HTML(text)
    # 获取每条航道通告的链接
    ul = html.xpath("//div[@class='gl_list1']/ul//li//a/@href")
    # print(ul)
    base_url = "http://www.cjhdj.com.cn/hdfw/hdtg/sy/"
    detail_urls = [base_url + i[2:] for i in ul]
    return detail_urls

# 获取航道通告具体的信息
def parse_detail_page(url):
    resp = requests.get(url, headers=headers)
    text = resp.content.decode('utf-8')
    html = etree.HTML(text)
    # 获取标题
    title = html.xpath("//div[@class='cn']/p[@class='biaoti']/text()")[0]
    print(title)
    # 获取时间
    time_list = html.xpath("//div[@class='cn']/div[@class='xl_tit2']/div")
    timestr = etree.tostring(time_list[0], encoding="gbk").decode("gbk")
    mat = re.search(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2})", timestr)
    times = mat.group(0)
    # 获取正文
    content_elem = html.xpath("//div[@class='cn']/div[@class='xl_tit2']/following-sibling::*")
    elem_content = etree.tostring(content_elem[0])
    # 发布机构
    soup = BeautifulSoup(elem_content, 'lxml')
    organize = soup.span.string
    # 存储内容
    content = ""
    for i in soup.find_all(name="p"):
        if i.get_text() == "":
            continue
        else:
            content = content + i.get_text() + "\n"
    for j in soup.find_all(name="span"):
        if j.get_text() == "":
            continue
        else:
            content = content + j.get_text() + "\n"
    # print(content)
    # 区间段
    scale = "上游"
    return title, times, organize, content, scale


def main():
    db = pymysql.connect(host="127.0.0.1", port=3306, user="root", password="123456", database="cjhdtg")
    cursor = db.cursor()
    # 长江航道局上游信息的抓取
    base_url = "http://www.cjhdj.com.cn/hdfw/hdtg/sy/"
    # pages_list = ["index.shtml"]
    pages_list = list()
    # 上游的信息目前抓取到第33页
    for i in range(33, 143):
        last_item = "index_%d.shtml" % i
        pages_list.append(last_item)
    for last_url in pages_list:
        url = base_url + last_url
        print("当前准备抓取:%s页面的内容" % url)
        # print(url)
        detail_urls = get_detail_urls(url)
        for detail_url in detail_urls:
            print("当前正在抓取%s页面" % detail_url)
            title, times, organize, content, scale = parse_detail_page(detail_url)
            time.sleep(5)
            sql = "insert into hdtg(title,scale, time, content, organization) values(%s, %s, %s, %s, %s)"
            cursor.execute(sql, (title, scale, times, content, organize))
            db.commit()
        time.sleep(30) # 休眠30秒之后继续抓取
    cursor.close()
    db.close()




if __name__ == '__main__':
    main()


