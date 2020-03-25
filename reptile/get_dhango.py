import requests
from bs4 import BeautifulSoup
re=requests.get('https://www.zmrenwu.com/courses/hellodjango-blog-tutorial/materials/59/')
soup = BeautifulSoup(re.text, 'lxml')
con = soup.find_all('div', class_='unit-1-4 unit-1-on-mobile scroll-view h-100')[0]
href=con.find_all('a')
print(href)
for row in href:
    print(row[0])

re.close()