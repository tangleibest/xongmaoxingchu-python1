import requests
from lxml import etree
from bs4 import BeautifulSoup

with open("带着联盟大招去修仙.txt", 'a', encoding='utf-8') as f:
    url = "https://www.166xs.cc/xiaoshuo/156086/"
    request = requests.get(url)
    html = request.text

    soup = BeautifulSoup(html, 'lxml')
    # con=soup.select('/html/body/div[4]/div[1]/div[1]/div[8]/div[2]/div[2]/ul/li[1]/div/p[1]/span[2]/b')
    html1 = etree.HTML(html)
    # #支付方式
    soupd = soup.find_all('div', id='list')[0].find_all('a')

    for row in soupd:
        every_url = row.get('href')
        title = row.text
        concat_url = 'https://www.166xs.cc/' + str(every_url)

        request_content = requests.get(concat_url)
        html2 = request_content.text
        f.write(title + '\n')
        soup2 = BeautifulSoup(html2, 'lxml')
        html2 = etree.HTML(html2)
        soupd2 = soup2.find_all('div', id='content')[0].text
        new_line = str(soupd2).replace('    ', '\n\n')
        # print(new_line)
        f.writelines(new_line)
        # f.write('\n\n')
