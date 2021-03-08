#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!pip install pyaspeller
import requests
import pandas as pd
from pyaspeller import YandexSpeller
import re
import _locale
_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])


access_token = '*****'  #OAuth-токен, как его получить: https://yandex.ru/dev/oauth/doc/dg/tasks/get-oauth-token.html
login = '****'  #клиентский рекламный логин в Директе 



# ## Получаем список включенных текстовых кампаний




url = 'https://api.direct.yandex.com/json/v5/campaigns'

headers = { 
    'Authorization': f'Bearer {access_token}', 
    'Client-Login' : login,
    "Accept-Language": "ru",
    "skipReportHeader": "true",
    "skipReportSummary": "true",
    "Use-Operator-Units": "true"  # Использование api-баллов агентства
}

body = {
  "method": "get",
  "params": { 
    "SelectionCriteria": { 
    "Types": ["TEXT_CAMPAIGN"],
    "States": ["ON"],
    }, 
    "FieldNames" : ["Id" , "Name", "State" , "Status", "Type"]
  } 
}


status = None

while status in {201, 202, None}:
    res = requests.post(url, headers=headers, json=body)
    status = res.status_code
    retryIn = res.headers.get('retryIn', None)
    reportsInQueue = res.headers.get('reportsInQueue', None)
    print(f'status = {status} wait {retryIn}. queue {reportsInQueue}')
    
    if retryIn:
        sleep(int(retryIn))

campaign_dict = {}

try:
    for camp in res.json()['result']['Campaigns']:
        for key in camp:
            if key not in campaign_dict:
                campaign_dict[key] = []
                campaign_dict[key].append(camp[key])
            else:
                campaign_dict[key].append(camp[key])

    camp_df = pd.DataFrame(campaign_dict)
    camp_df
    
except KeyError:
    print(res.json()['error']['error_detail'])





camp_df


# ## Все объявления из всех кампаний




campaigns = camp_df['Id'].unique().tolist()





ads_dict = {}

n = 1

for camp in campaigns:
    print(f'CampaignId: {camp}, {n}/{len(campaigns)}')
    
    url = 'https://api.direct.yandex.com/json/v5/ads'
    
    body = {
          "method": "get",
          "params": { 
            "SelectionCriteria": { 

                "CampaignIds" : [camp]

            }, 
            "FieldNames" : [ "Id" , "State", "CampaignId"] ,
             "TextAdFieldNames": ["Text" , "Title" , "Title2",
                                  "AdExtensions", "SitelinkSetId",
                                  "AdImageModeration", "SitelinksModeration"],


          } 
        } 


    

    status = None
    while status in {201, 202, None}:
        res = requests.post(url, headers=headers, json=body)
        status = res.status_code
        retryIn = res.headers.get('retryIn', None)
        reportsInQueue = res.headers.get('reportsInQueue', None)
        if retryIn:
            sleep(int(retryIn))

    try:
        for ad in res.json()['result']['Ads']:
            if 'TextAd' in ad:
                for key in ad:

                    if key not in ads_dict:
                        ads_dict[key] = []
                        ads_dict[key].append(ad[key])


                    else:
                        ads_dict[key].append(ad[key])
            else:
                continue
                
                
    except KeyError:
        continue
    
    n += 1
            


ads_df = pd.DataFrame(ads_dict)
print(f'\nКол-во объявлений: {len(ads_df)}')

ads_df.head()





ads_df = ads_df[ads_df['State'] == 'ON'][['Id','TextAd','CampaignId']] # Только активные объявления
# ads_df = ads_df[['Id','TextAd','CampaignId']] # Все
print(f'Кол-во активных объявлений: {len(ads_df)}')





# Выдащим все тексты

texts_dict = {'Id': [], 'CampaignId' : []}


for x, y in ads_df.iterrows():
    
    texts_dict['Id'].append(y[0])
    texts_dict['CampaignId'].append(y[2])    
    
    for key in y[1]:
        
        if key not in texts_dict:
            texts_dict[key] = []
            texts_dict[key].append(y[1][key])           
           
        else:
            texts_dict[key].append(y[1][key])

        
txtdata = pd.DataFrame(texts_dict)
txtdata = txtdata.fillna('')
txtdata.head()


# ## ПАРСИМ УНИКАЛЬНЫЕ ЗАГОЛОВКИ




all_titles = txtdata['Title'].unique().tolist()
print('')
print(f'Уникальных заголовков: {len(all_titles)}\n')

for title in all_titles:
    print(title)


# ##  ПАРСИМ ВТОРЫЕ ЗАГОЛОВКИ




all_titles2 = txtdata['Title2'].unique().tolist()
print('')
print(f'Уникальных вторых заголовков: {len(all_titles2)}\n')

for title2 in all_titles2:
    print(title2)


# ## ПАРСИМ ТЕКСТЫ




all_txt = txtdata['Text'].unique().tolist()
print('')
print(f'Уникальных текстов: {len(all_txt)}\n')

for txt in all_txt:
    print(txt)

    
# ## ПАРСИМ  БС


# Сперва найдём уникальные ID быстрых ссылок

SitelinkSetIds = txtdata['SitelinkSetId'].unique().tolist()
print('')
print(f'Количество наборов быстрых ссылок: {len(SitelinkSetIds)}')



# [int(x) for x in SitelinkSetIds if x != '']



body = {
      "method": "get",
      "params": { 
        "SelectionCriteria": { 
                
        "Ids" : [int(x) for x in SitelinkSetIds if x != '']


        }, 
        "FieldNames" : [ "Id" ],
 "SitelinkFieldNames": [ "Title" , "Href" , "Description"]


      } 
    } 

headers = { 
        'Authorization': f'Bearer {access_token}', 
        'Client-Login' : login,
        "Accept-Language": "ru",
        "skipReportHeader": "true",
        "skipReportSummary": "true"
    }

url = 'https://api.direct.yandex.com/json/v5/sitelinks'

status = None
while status in {201, 202, None}:
    res = requests.post(url, headers=headers, json=body)
    status = res.status_code
    retryIn = res.headers.get('retryIn', None)
    reportsInQueue = res.headers.get('reportsInQueue', None)
    if retryIn:
        sleep(int(retryIn))


sl_dict = {}

try:
    for ad in res.json()['result']['SitelinksSets']:
        for key in ad:
            if key not in sl_dict:
                sl_dict[key] = []
                sl_dict[key].append(ad[key])
            else:
                sl_dict[key].append(ad[key])
except:
    pass


sl_df = pd.DataFrame(sl_dict)

sitelinks_dict = {'SitelinkId': []}
for x, y in sl_df.iterrows():
    for ad in y[1]:
        sitelinks_dict['SitelinkId'].append(y[0])
        for key in ad:
            if key not in sitelinks_dict:
                sitelinks_dict[key] = []
                sitelinks_dict[key].append(ad[key])
            else:
                sitelinks_dict[key].append(ad[key])



sitelinks_df = pd.DataFrame(sitelinks_dict).fillna('')


print('')

print('Заголовки БC:')

print('')

for x in sitelinks_df['Title']:
    print(x)

print('')    
    
print('Описания БC:')

print('') 
for x in sitelinks_df['Description']:
    print(x)
    
print('')       
    
### ПАРСИМ РАСШИРЕНИЯ    

AdExtensions = []
for ext in txtdata['AdExtensions']:
    for e in ext:
        ex_id = e['AdExtensionId']
        if ex_id not in AdExtensions:
            AdExtensions.append(ex_id)

            
            
print('')         
print(f'Количество уникальных уточнений: {len(AdExtensions)}')            

print('')



url = 'https://api.direct.yandex.com/json/v5/adextensions'

body = {
      "method": "get",
      "params": { 
        "SelectionCriteria": { 
                
        "Ids" : AdExtensions


        }, 
          
          "CalloutFieldNames": ["CalloutText"],
          "FieldNames": ["Id", "Associated", "Status", "State"]


      
    }
}

headers = { 
        'Authorization': f'Bearer {access_token}', 
        'Client-Login' : login,
        "Accept-Language": "ru",
        "skipReportHeader": "true",
        "skipReportSummary": "true"
    }


status = None
while status in {201, 202, None}:
    res = requests.post(url, headers=headers, json=body)
    status = res.status_code
    retryIn = res.headers.get('retryIn', None)
    reportsInQueue = res.headers.get('reportsInQueue', None)
    if retryIn:
        sleep(int(retryIn))





ext_dict = {}

for ad in res.json()['result']['AdExtensions']:
    for key in ad:
        if key not in ext_dict:
            ext_dict[key] = []
            ext_dict[key].append(ad[key])
        else:
            ext_dict[key].append(ad[key])

ext_df = pd.DataFrame(ext_dict)
ext_df['Callout'] = ext_df['Callout'].map(lambda x: x['CalloutText'])


all_unique_extensions = ext_df['Callout'].unique().tolist()

for x in all_unique_extensions:
    print(x)
    
    
# ## НАЧИНАЕМ ПРОВЕРКУ НА ОПЕЧАТКИ

speller = YandexSpeller()
 
print('') 
print('НАЧИНАЮ ПРОВЕРКУ ОПЕЧАТОК:')    
print('') 

print('Чекю заголовки...')
print('') 


# ## Чеким заголовки 


error_titles = []

all_unique_titles = txtdata['Title'].unique().tolist()
words_to_check = []

for text in all_unique_titles:     
    text = re.sub('\s(.)', r' \1', text) # Т.к. может ругаться на неразрывные пробелы    
    for word in text.split(' '): 
        if word not in words_to_check:
            words_to_check.append(word)


for word in words_to_check:
    fixed = speller.spelled(word)
    if word != fixed:        
        g = (word + ' => ' + fixed)
        print(g)
        error_titles.append(g)
   
df_titles = pd.DataFrame(error_titles)

if len(df_titles) > 0:
    df_titles = df_titles[0].str.split('=>', expand=True)
    df_titles.columns=['Заголовки_как сейчас:','Предлагаю заменить на:'] 
else:
    df_titles = pd.DataFrame({'хей': ['Очепяток в загах'], 'хой': ['Не нашёл!']})
    print(df_titles)


# ### Чеким Title2

print('') 

print('Чекю подзаголовки...')

print('') 




error_titles2 = []

all_unique_titles2 = txtdata['Title2'].unique().tolist()
words_to_check = []

for text in all_unique_titles2:     
    text = re.sub('\s(.)', r' \1', text) # Т.к. может ругаться на неразрывные пробелы 
    text = re.sub(' \.', r'', text)  # Т.к. может ругаться на " ."
    for word in text.split(' '): 
        if word not in words_to_check:
            words_to_check.append(word)

for word in words_to_check:    
    fixed = speller.spelled(word)
    if word != fixed:        
        g = (word + ' => ' + fixed)
        print(g)
        error_titles2.append(g)
        
df_titles2 = pd.DataFrame(error_titles2)
if len(df_titles2) > 0:
    df_titles2 = df_titles2[0].str.split('=>', expand=True)
    df_titles2.columns=['Подзаголовки_как сейчас:','Предлагаю заменить на:'] 
else:
    df_titles2 = pd.DataFrame({'хей': ['Очепяток в подзагах'], 'хой': ['Не нашёл!']})
    print(df_titles2)


# ### Чеким опечатки в текстах

print('') 

print('Чекю текста...')

print('') 



error_texts = []

all_unique_texts = txtdata['Text'].unique().tolist()
words_to_check = []


for text in all_unique_texts:     
    text = re.sub('\s(.)', r' \1', text) # Т.к. может ругаться на неразрывные пробелы 
    text = re.sub(' \.', r'', text)  # Т.к. может ругаться на " ."
    for word in text.split(' '): 
        if word not in words_to_check:
            words_to_check.append(word)

for word in words_to_check: 
    #print(word)
    fixed = speller.spelled(word)
    if word != fixed:        
        g = (word + ' => ' + fixed)
        print(g)
        error_texts.append(g)
        
df_texts = pd.DataFrame(error_texts)
if len(df_texts) > 0:
    
    df_texts = df_texts[0].str.split('=>', expand=True)
    df_texts.columns=['Тексты_как сейчас:','Предлагаю заменить на:'] 
else:
    df_texts = pd.DataFrame({'хей': ['Очепяток в текстах'], 'хой': ['Не нашёл!']})
    print(df_texts)
    
    
# ### Чеким опечатки в Sitelinks



print('') 

print('Чекю заголовки быстрых ссылок...')

print('') 


error_sl_titles = []
all_unique_sl_titles = sitelinks_df['Title'].unique().tolist()
words_to_check = []


for text in all_unique_sl_titles:     
    text = re.sub('\s(.)', r' \1', text) # Т.к. может ругаться на неразрывные пробелы \
    text = re.sub(' \.', r'', text)  # Т.к. может ругаться на " ."
    for word in text.split(' '): 
        if word not in words_to_check:
            words_to_check.append(word)

for word in words_to_check:    
    fixed = speller.spelled(word)
    if word != fixed:        
        g = (word + ' => ' + fixed)
        print(g)
        error_sl_titles.append(g)
        
        
df_titles_sl = pd.DataFrame(error_sl_titles)
if len(df_titles_sl) > 0:
    df_titles_sl = df_titles_sl[0].str.split('=>', expand=True)
    df_titles_sl.columns=['БС_заги_как сейчас:','Предлагаю заменить на:'] 
else:
    df_titles_sl = pd.DataFrame({'хей': ['Очепяток в заголовках БС не нашел'], 'хой': ['Не нашёл! <(￣︶￣)>']})
    print(df_titles_sl) 
    
    
print('') 

print('Чекю описания быстрых ссылок...')

print('') 


error_sl_descriptions = []
all_unique_sl_descriptions = sitelinks_df['Description'].unique().tolist()
words_to_check = []


for text in all_unique_sl_descriptions:     
    text = re.sub('\s(.)', r' \1', text) # Т.к. может ругаться на неразрывные пробелы  
    text = re.sub(' \.', r'', text)  # Т.к. может ругаться на " ."
    text = re.sub('(?P<url>https?://[^\s]+)', r'ССЫЛКА', text) # Т.к. может ругаться на ссылку в Description
    for word in text.split(' '): 
        if word not in words_to_check:
            words_to_check.append(word)

for word in words_to_check:    
    fixed = speller.spelled(word)
    if word != fixed:        
        g = (word + ' => ' + fixed)
        print(g)
        error_sl_descriptions.append(g)
        
        
df_descriptions_sl = pd.DataFrame(error_sl_descriptions)
if len(df_descriptions_sl) > 0:
    df_descriptions_sl = df_descriptions_sl[0].str.split('=>', expand=True)
    df_descriptions_sl.columns=['БС_описания_как сейчас:','Предлагаю заменить на:'] 
else:
    df_descriptions_sl = pd.DataFrame({'хей': ['Очепяток в описаниях БС не нашел'], 'хой': ['Не нашёл!']})
    print(df_descriptions_sl) 
    
    
    
# ## Проверка уточнений



print('') 

print('Чекю расширения...')

print('') 


error_extensions = []

all_unique_extensions = ext_df['Callout'].unique().tolist()
words_to_check = []




for text in all_unique_extensions:     
    text = re.sub('\s(.)', r' \1', text) # Т.к. может ругаться на неразрывные пробелы  
    text = re.sub(' \.', r'', text)  # Т.к. может ругаться на " ."
    for word in text.split(' '): 
        if word not in words_to_check:
            words_to_check.append(word)

for word in words_to_check:    
    fixed = speller.spelled(word)
    if word != fixed:        
        g = (word + ' => ' + fixed)
        print(g)
        error_extensions.append(g)
        
        
df_extensions = pd.DataFrame(error_extensions)
if len(df_extensions) > 0:
    df_extensions = df_extensions[0].str.split('=>', expand=True)
    df_extensions.columns=['В расширениях_сейчас:','Предлагаю заменить на:'] 
else:
    df_extensions = pd.DataFrame({'хей': ['Очепяток в расширениях не нашел'], 'хой': ['Не нашёл!']})
    print(df_extensions) 

    

writer = pd.ExcelWriter('Чекер_ОПЕПЯТОК_ЯД.xlsx', engine='xlsxwriter')

df_titles.to_excel(writer, sheet_name='Заголовки',index=False)
df_titles2.to_excel(writer, sheet_name='Подзаголовки',index=False)
df_texts.to_excel(writer, sheet_name='Тексты',index=False)
df_titles_sl.to_excel(writer, sheet_name='Заги_БС',index=False)
df_descriptions_sl.to_excel(writer, sheet_name='Описания_БС',index=False)
df_extensions.to_excel(writer, sheet_name='Расширения',index=False)

writer.save() 

print('')
print('ГОТОВО!')

