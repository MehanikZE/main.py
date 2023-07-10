import lxml

# import record as record
import requests
import aiohttp
import psycopg2
import asyncio
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

user_agent = {"User-agent": "Mozilla/5.0"}
url_api ='https://api.hh.ru/vacancies?text=python%20middle&per_page=100'
DB_USER = "postgres"
DB_NAME = "hw1"
DB_PASSWORD = "Vsc341zh"
DB_HOST = "127.0.0.1"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

url = "https://hh.ru/search/vacancy?text=middle+python+developer&area=1&items_on_page=3&page=5&st=vacancy_simple"

class Base(DeclarativeBase):
    pass
class Vacan(Base):
    __tablename__ = "vacancies"

    index: Mapped[str] = mapped_column(primary_key=True)
    company_name: Mapped[str]
    position: Mapped[str]
    job_description: Mapped[str]
    key_skills: Mapped[str]

    def __repr__(self) -> str:
        return f"User(id={self.index!r}, name={self.company_name!r}, fullname={self.position!r}, fullname={self.key_skills!r})"

class Vacan_bs4(Base):
    __tablename__ = "vacancies_bs4"

    index: Mapped[str] = mapped_column(primary_key=True)
    company_name: Mapped[str]
    position: Mapped[str]
    job_description: Mapped[str]
    key_skills: Mapped[str]

    def __repr__(self) -> str:
        return f"User(id={self.index!r}, name={self.company_name!r}, fullname={self.position!r}, fullname={self.key_skills!r})"


engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
print("Таблица создана")


result1 = requests.get(url, headers=user_agent)
print(result1.status_code)
b = result1.content.decode()

soup = BeautifulSoup(b, "lxml")
soup_en = soup.encode()

list = []
for link in soup.find_all("a"):
    s = link.get("href")
    list.append(s)
    print(link.get("href"))
print(list)
list_h = []
for i in list:
    if i[:30] == "https://voronezh.hh.ru/vacancy":
        list_h.append(i)


for sdf, z in enumerate(list_h):
    url_s = z
    resultat = requests.get(url_s, headers=user_agent)
    k = resultat.content.decode()
    sop = BeautifulSoup(k, "lxml")
    nazvan = sop.find_all("h1", {"data-qa": "vacancy-title"})
    value1 = nazvan[0].getText()

    description = sop.find_all("div", {"data-qa": "vacancy-description"})
    value2 = description[0].getText()

    company_name = sop.find_all("span", {"data-qa": "bloko-header-2"})
    value3 = company_name[0].getText()

    key_skils = sop.find_all("span", {"data-qa": "bloko-tag__text"})
    lis_skill = []
    for x in key_skils:
        print(f'aaa', x)
        print(type(x))
        skils_ed = x.text
        lis_skill.append(skils_ed)
    print(f'spis', lis_skill)

    with Session(engine) as session:
        names_bs4 = Vacan_bs4(
            index=sdf+1,
            company_name=value3,
            position=value1,
            job_description=value2,
            key_skills=lis_skill,
        )
        session.add_all([names_bs4])
        session.commit()

###################################################################################################
print(f'Парсинг через API START')

result = requests.get(url_api, headers=user_agent)

print(result.status_code)
print(result.text)
j = result.json()
print('dddd',j)
print(type(j))
vacans = result.json().get('items')
print('eeeee',vacans)
ttt = []
for i, vac in enumerate(vacans):
    print(i+1) #vac['name'], vac['url'], vac['alternate_url'])
    s = vac['url']
    print(s)
    res = requests.get(s, headers=user_agent)
    vacs = res.json()
    m = vacs['name']
    g = vacs['employer']['name']
    z = vacs['description']
    key_skills = vacs['key_skills']
    if key_skills:
        list = []
        for sk in key_skills:
            # list = []
            l = sk['name']
            list.append(l)
    else:
        print("xczcszcsdcs")
        list = "No skills in vacancy"
    with Session(engine) as session:
        names = Vacan(index = i+1 ,company_name = g, position = m, job_description = z, key_skills = list )
        session.add_all([names])
        session.commit()


    print(f'Комания:',g)
    #
##################################################################################################################


# async def main(ids):
#
#     async with aiohttp.ClientSession('https://api.hh.ru') as session:
#         task = []
#         for id in ids:
#             task.append(asyncio.create_task(get_vacansy(id, session)))
#         results = await asyncio.gather(*task)
#     for l, result in enumerate (results):
#         k = result['name']
#         # if k is not True:
#         #     print('xx')
#         print(l+1, result['name'],  result['employer']['name'], result['description']) #, result['key_skills'])


# async with session.get('https://www.voronezh.hh.ru') as response:
#     print(f"Ответ", response.status)
#     # print(f'респонс',response)
#     html = await response.text()
#     return html
# print(html[:150])
# vacansies_ids =  ['82525826', '82402579']
# print()
# asyncio.run(spisok())

