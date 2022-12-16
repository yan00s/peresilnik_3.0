import asyncio
import os
import sqlite3 as sql
import random
import re
import vk_api
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from vk_api.bot_longpoll import VkBotLongPoll
from vk_api.bot_longpoll import VkBotEventType
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.errors import UserAlreadyParticipantError
from dotenv import load_dotenv,find_dotenv#,set_key
from os import environ
from fake_headers import Headers
import aiohttp
import logging
import aiofiles
import json
import time
__author__ = "https://t.me/yan00s"
__version__ = "3.0"

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.basicConfig(encoding='utf-8', level=logging.DEBUG,
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='errors.logs',
                    filemode='a')

def get_perids_groups_vk_all():
  req = 'SELECT DISTINCT perid_group FROM added_groups_vk WHERE included == 1'
  perids_groups = cursor.execute(req).fetchall()
  perids_groups = map(lambda x:x[0], perids_groups)
  return list(perids_groups)

def help():
  text = 'Для того что бы добавить ссылку в отслеживаемые\nВведи ссылку на телеграм' \
          '\nс "!add" пример:\n"!add t.me/aboba"\n' \
          '\nдобавление приватных тг каналов: !add *линк* @*название*'\
          '\nпример:\n!add https://t.me/joinchat/A3A231EmqtEx @инсайдОлег\n'\
          '\n!tg отслеживаемые каналы тг\n' \
          '!twitt отслеживаемые твиты\n' \
          '\nдля того что бы добавить себя в упоминания введи:\n!упом *канал*\n' \
          '\nдля того что бы не упоминать:\n!неупом *канал* (ALL удаляет всё)\n'\
          '\n!deltg удаляет канал пример:\n!deltg @gege или !deltg t.me/gege' \
          '\n!deltwitt удаляет отслеживаемый твиттер, пример:\n!deltwitt https://twitter.com/3aiH323232fav23A\n'\
          '!peerid- !peerid+ !peerids'
  return text


def channels_upd(type,peerid_vk):
  rerp = f"""SELECT DISTINCT link,perid FROM links WHERE type='{type}' AND peerid_group_vk='{peerid_vk}'"""
  channels_ = []
  channel_data = {}
  result = cursor.execute(rerp).fetchall()
  for login0, perid in result:
    if type == 'tg':
      channels_.append(login0)
    elif type == 'twitt':
      channels_.append(perid)
    channel_data.update({perid:login0})
  return channels_,channel_data


def upd_channels_tg(peerid_vk):
  ch = channels_upd('tg',peerid_vk)
  if ch[0]:
    return ch
  else:
    ch = ['@NONE'],{"0":'@NONE'}
    return ch

def upd_channels_tg_all():
  reqst = "SELECT link,perid FROM links WHERE type='tg'"
  results = cursor.execute(reqst).fetchall()
  channels_ = []
  channel_data = {}
  for result in results:
    login0 = result[0]
    perid = result[1]
    channels_.append(login0)
    channel_data.update({perid:login0})
  if channels_ == [] and channel_data == {}:
    ch = ['@NONE'],{"0":'@NONE'}
    return ch
  return channels_,channel_data


def upd_twitter(peerid_vk):
  ch = channels_upd('twitt',peerid_vk)
  if ch[0]:
    return ch
  else:
    ch = ['@NONE'],{"0":'@NONE'}
    return ch


def upd_twitter_all():
  reqst = "SELECT DISTINCT link, perid, last_post_id FROM links WHERE type='twitt'"
  results = cursor.execute(reqst).fetchall()
  channel_data = {}
  for result in results:
    login0 = result[0]
    perid = result[1]
    last_post_id = result[2]
    channel_data[perid] = {'link':login0, 'last_post_id':last_post_id}
  return channel_data


async def add_twitt(link,peerid_vk):
  if not check_in_bd('twitt',link,peerid_vk):
    login = str(link).split('/')[-1]
    data = {'input':login}
    headers = header.generate()
    async with aiohttp.ClientSession(headers=headers) as session:
      req = await session.post('https://tweeterid.com/ajax.php',data=data)
      peerid = await req.text()
    if not peerid == 'error':
      req = f"INSERT INTO links VALUES ('twitt','{link}','{peerid}','{peerid_vk}', 0)"
      cursor.execute(req)
      text = "Добавлено в базу данных"
    else:
      text = "Произошла ошибка, проверь правильность введенных данных"
  else:
    text = "Уже добавлен в базу"
  conn.commit()
  return text


async def join_channel(chanel,private = False):
  try:
    client1 = TelegramClient(path_session1, api_id, api_hash)
    await client1.connect()
    if not private:
      result = await client1(JoinChannelRequest(chanel))
      chanelid = result.chats[0].id
    if private:
      try:
        if '+' in chanel:
          chanel = str(chanel).split("+")[-1]
        result = await client1(ImportChatInviteRequest(hash = chanel))
        chanelid = result.chats[0].id
      except UserAlreadyParticipantError:
        result = await client1(CheckChatInviteRequest(hash = chanel))
        result = result.to_json()
        result = json.loads(result)
        chanelid = result['chat']['id']
      except Exception as e:
        chanelid = {'ERROR':e}
    await client1.disconnect()
    return True,chanelid
  except Exception as e:
    try:
      if await client1.is_user_authorized():
        await client1.disconnect()
    except: pass
    return False


def check_in_bd_all(link):
  reqst = "SELECT link FROM links"
  results = cursor.execute(reqst).fetchall()
  channels = []
  [channels.append(x[0]) for x in results]
  if link in channels:
    return True
  else:
    return False

def get_peerid_tg(channel):
  reqst = f"SELECT perid FROM links where link='{channel}'"
  result = cursor.execute(reqst)
  return list(result)[0][0]

async def add_telega(link,peerid_vk, is_private = False):
  stop_tg = False
  if not is_private:
    name_tg = "@"+str(link).split('/')[-1]
  else:
    name_tg, tg = link
  type_ = 'tg'
  if check_in_bd_all(name_tg) and not check_in_bd(type_,name_tg,peerid_vk):
    peerid = get_peerid_tg(name_tg)
    reqs = f"INSERT INTO links VALUES ('{type_}','{name_tg}',{peerid},{peerid_vk}, 0)"
    cursor.execute(reqs)
    conn.commit()
    text = "Добавлено в базу данных"
    return text,stop_tg
  elif not check_in_bd(type_,name_tg,peerid_vk):
    if is_private:
      success = await join_channel(name_tg,is_private)
      name_tg = f"@{tg}"
    else:
      success = await join_channel(name_tg,is_private)
    if success:
      stop_tg,peerid = success
      if stop_tg and not type(peerid) is dict:
        success,peerid = success
        cursor.execute(f"INSERT INTO links VALUES ('{type_}','{name_tg}',{peerid},{peerid_vk}, 0)")
        conn.commit()
        text = "Добавлено в базу данных, перезапускаю телеграм таск..."
        stop_tg = True
      elif 'ERROR' in peerid:
        err = list(peerid.values())[0]
        text = f'произошла ошибка:\n{err}'
    else:
      text = 'Не получилось вступить в канал'
  else:
    text = "Уже добавлен в базу"
  return text,stop_tg


def del_twitt(link,peerid_vk):
  type_ = 'twitt'
  if check_in_bd(type_,link,peerid_vk):
    cursor.execute(f"DELETE FROM links WHERE link='{link}' AND peerid_group_vk='{peerid_vk}'")
    conn.commit()
    text = "удалено из базы данных"
  else:
    text = "Нету в базе данных"
  return text


def check_in_bd(type_,link,peerid_vk):
  if type_ == 'tg':
    if link in channels_upd(type_,peerid_vk)[0]:
      return True
  if type_ == 'twitt':
    if link in list(channels_upd(type_,peerid_vk)[1].values()):
      return True
  return False

def deltg(link,peerid_vk):
  stop_tg = False
  # from telethon.tl.functions.channels import LeaveChannelRequest
  # client(LeaveChannelRequest(input_channel))
  if not "@" in link:
    tg = "@"+str(link).split('/')[-1]
  else: tg = str(link).split(' ')[-1]
  type_ = 'tg'
  if check_in_bd(type_,tg,peerid_vk):
    cursor.execute(f"DELETE FROM links WHERE link='{tg}' AND peerid_group_vk='{peerid_vk}'")
    conn.commit()
    if check_in_bd_all(tg):
      text = 'удалено из базы данных'
    else:
      stop_tg = True
      text = "удалено из базы данных, перезапускаю телеграм таск..."
  else:
    text = "Нету в базе данных"
  return text,stop_tg

def upload_photo(photo):
  upload = vk_api.VkUpload(vk)
  attachment = upload.photo_messages(photos = photo)
  return attachment
def name_photo():
  symb = '1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM'
  name = ''.join(random.choice(symb) for _ in range(7))
  name = f"/photo//{name}.jpg"
  return name


def get_notice_from_channel(channel,peerid):
  peerid = str(peerid)
  req = f'SELECT user_peerid FROM notion_users WHERE perid_group == "{peerid}" AND link == "{channel}"'
  result = cursor.execute(req).fetchall()
  notice_ = []
  [notice_.append(x[0]) for x in result]
  return notice_


def notion_del_add(id,peerid, ADD=False, ALL=False, channel = None):
  req = f'SELECT link FROM notion_users WHERE perid_group == "{peerid}" AND user_peerid == "{id}"'
  result_links = cursor.execute(req).fetchall()
  result_links = map(lambda x:x[0], result_links)
  id,peerid = str(id),str(peerid)
  channel = str(channel) if not channel is None else None
  type_channel = 'tg' if not 'twitter.com' in channel else 'twitt'
  ALL = True if channel == 'all' else False
  if ADD:
    if ALL:
      t = 'введи канал для включения уведомлений'
    elif channel in result_links:
      t = f'уведомления для {channel} уже были включены'
    else:
      req = f'INSERT INTO notion_users VALUES ("{peerid}", "{id}", "{type_channel}", "{channel}")'
      cursor.execute(req)
      t = f'уведомления для {channel} включены'
  else:
    if ALL:
      req = f'DELETE FROM notion_users WHERE perid_group == "{peerid}" AND user_peerid == "{id}"'
      cursor.execute(req)
      t = 'уведомления для всех каналов удалены'
    else:
      try:
        if channel in result_links:
          req = f'DELETE FROM notion_users WHERE perid_group == "{peerid}" AND user_peerid == "{id}" AND link == "{channel}"'
          cursor.execute(req)
          t = f'уведомления для {channel} удалены'
        else:
            t = f'уведомления для {channel} не были включены'
            return t
      except KeyError:
        t = f'уведомления для {channel} не включены'
  conn.commit()
  return t


def get_peerids_from_channels(channel):
  res = f"""SELECT DISTINCT peerid_group_vk FROM links WHERE link='{channel}'"""
  result = cursor.execute(res).fetchall()
  peerids_groups_vk = []
  [peerids_groups_vk.append(perid[0]) for perid in result]
  return peerids_groups_vk


def id_filter(chats):
  chatid_for_client = []
  for chat in chats:
    chatid_for_client.append(int(f'-100{chat}'))
  return chatid_for_client

async def tg_start():
  while True:
    try:
      # from telethon import TelegramClient, events
      client = TelegramClient(path_session, api_id, api_hash)
      channels = upd_channels_tg_all()[0]
      if channels[0] == '@NONE':
        return
      check_username = upd_channels_tg_all()[1]
      channels_for_client = id_filter(check_username)
      @client.on(events.NewMessage(chats=channels_for_client))
      async def checker_msgtg(event):
        if event.message:
          attachment = None
          user = check_username[str(event.peer_id.channel_id)]
          if event.message.photo:
            name = name_photo()
            await client.download_media(event.message, name) # await
            attachment = upload_photo(name)
            os.remove(name)
            owner_id = attachment[0]["owner_id"]
            item_id = attachment[0]["id"]
            attachment = f"photo{owner_id}_{item_id}"
          links_raw = event.message.entities
          links_text = ''
          if links_raw:
            links = []
            for link in links_raw:
              if hasattr(link, 'url'):
                links.append(link.url)
            links_t = '\n'.join(links)
            links_text = f'\n ссылки:\n{links_t}' if len(links_t) > 0 else ''
          perids_groups = get_peerids_from_channels(user)
          for perid_group in perids_groups:
            t0 = get_notice_from_channel(user,perid_group)
            t0 = '\n'+','.join(map(lambda x:'@id' + x,t0)) if not t0 == [] else ''
            text = f"telegram {user}{t0}\n{str(event.message.message)}{links_text}"
            if text == f'telegram {user}\n' and attachment is None:
              text = f'{text}*(опрос/видео/статья)*'
            await send_m(perid_group,text,attachment = attachment)
            await asyncio.sleep(0.25)
      await client.start()
      await client.run_until_disconnected()
    except asyncio.CancelledError:
      await client.disconnect()
      await asyncio.sleep(2)
      continue
    except Exception as e:
      err = f'error on telegram = {e}'
      logging.exception(err)
      await asyncio.sleep(7)

async def send_m(peerid,message,attachment = None):
  return vk.messages.send(random_id=0,peer_id=peerid,message=message,attachment=attachment)

def twitt_lister(peerid):
  t = upd_twitter(peerid)
  t0 = t[0]
  if t0:
    if t0[0] == '@NONE':
      t0 = 'отслеживаемых twitter нету'
    else:
      t0 = list(t[1].values())
      t0 = ', '.join(t0)
  return t0


async def adder(mtext, peerid, result = False, private = False):
  text = 'не проиндексировано'
  if re.match(r'!add (.*) @(.*)', mtext):
    shit = 0
    private = True
  elif re.match(r'!add (.*) \[(.*)', mtext):
    shit = 1
    private = True
  try:
    if private:
      if shit == 0:
        mtext = "!add https://t.me/joinchat/A3A231EmqtEx @abob"
        refind = re.findall(r'!add (.*) @(.*)', mtext)[0]
      elif shit == 1:
        mtext = "!add https://t.me/joinchat/A3A231EmqtEx [club207686626|@public207686626]"
        refind = re.findall(r'!add (.*) \[.*@(.*)]', mtext)[0]
      text,result = await add_telega(refind,peerid, private)
    else:
      link = re.findall(r'!add (.*)', mtext)[0]
      if 'twitter' in link:
        text = await add_twitt(link,peerid)
      elif any(['tg' in link,'telegram' in link,'t.me' in link]):
        text,result = await add_telega(link,peerid, private)
  except Exception as e:
    t = f'ошибка на добавлении тг/твиттера \n{e}'
    logging.exception(t)
  return result, text




def notifed(mtext, peerid, id, add):
  text = 'введи логин канала или ссылку твиттера для уведомлений, пример: !упом @aboba'
  try:
    if add is True:
      channel = re.findall(r'!упом (.*)', mtext)[0]
    else:
      channel = re.findall(r'!неупом (.*)', mtext)[0]
    if '[' in channel:
      channel = str(channel).split('|')[-1][:-1].strip()
    elif any(['tg' in channel,'telegram' in channel,'t.me' in channel]):
      return text
  except IndexError:
    text = 'не проиндексировано'
    return text
  if "@" not in channel and not 'twitter.com' in channel and 'all' not in channel:
    return text
  else:
    text = notion_del_add(id=id,peerid=peerid, channel=channel, ADD=add)
  return text

def tg_list_checker(peerid):
  t = upd_channels_tg(peerid)[0]
  if t:
    if t[0] == '@NONE' and len(t) == 1:
      t = 'отслеживаемых телеграм каналов нету'
  return t

def delete_tg(mtext, peerid):
  text = 'не проиндексированно'
  result = False
  try:
    link:str = re.findall(r'!deltg (.*)', mtext)[0]
  except IndexError:
    return result, text
  if '[' in link:
    link = link.split('|')[-1][:-1].strip()
  if any(['tg' in link,'telegram' in link,'t.me' in link or '@' in link]):
    text,result = deltg(link,peerid)
  return result, text

def delete_twitt(mtext, peerid):
  try:
      link = re.findall(r'!deltwitt (.*)', mtext)[0]
  except IndexError:
    return 'не проиндексированно'
  if 'twitter' in link:
    text = del_twitt(link,peerid)
  return text


def peerid_group_delladd(peerid_group, included):
  req = f"UPDATE added_groups_vk SET included = {included} WHERE perid_group = {peerid_group}"
  cursor.execute(req)
  conn.commit()

def add_peerid(msg, fromid, admin_id):
  t = 'only for admin bot'
  if int(fromid) == admin_id:
    try:
      peerid_group = re.findall(r'!peerid\+ (.*)', msg)[0]
    except IndexError:
      return 'не проиндексированно'
    peerid_group_delladd(peerid_group, 1)
    t = f'Беседа с peerid = {peerid_group} включена'
  return t

def dell_peerid(msg, fromid, admin_id):
  t = 'only for admin bot'
  if int(fromid) == admin_id:
    try:
      peerid_group = re.findall(r'!peerid\- (.*)', msg)[0]
    except:
      return 'не проиндексированно'
    peerid_group_delladd(peerid_group, 0)
    t = f'Беседа с peerid = {peerid_group} выключена'
  return t

def select_category(mtext:str):
  if '!помощь' in mtext or '!help' in mtext:
    return 'HELP'
  if '!tg' in mtext:
    return 'TGLIST'
  if '!twitt' in mtext:
    return "TWITTLIST"
  if '!add' in mtext:
    return 'ADD'
  if "!упом" in mtext:
    return 'NOTIFED'
  if '!неупом' in mtext:
    return 'DELNOTIFED'
  if '!тест' in mtext or '!test' in mtext:
    return 'TESTED'
  if '!deltg' in mtext:
    return 'DELLTG'
  if '!deltwitt' in mtext:
    return 'DELLTWITT'
  if '!peerid+' in mtext:
    return 'ADDPEERID'
  if '!peerid-' in mtext:
    return 'DELLPEERID'
  if '!peerids' in mtext:
    return 'LISTPEERIDS'
  return False

def get_all_peerids(actual_peerid):
  req = "SELECT * FROM added_groups_vk"
  groups = cursor.execute(req).fetchall()
  clear_text = []
  for group in groups:
    peerid, included, who_add, timeadd = group
    time_aft = time.time()-timeadd
    text = f"PEERID = {peerid}\nВключен = {included} (1 = включен, 0 = выключен)\n"\
      f"Кто добавил = @id{who_add}\nВремя = {time.ctime(timeadd)}\nВремени прошло с момента добавления: {int(time_aft/60)} минут"
    if int(actual_peerid) == int(peerid):
      text = f"Текущая:\n{text}"
    clear_text.append(text)
  return "\n\n".join(clear_text)

async def selected_category(category, peerid, mtext, id, admin_id):
  result = False
  if category == 'HELP':
    text = help()
  elif category == 'TGLIST':
    text = tg_list_checker(peerid)
  elif category == 'TWITTLIST':
    text = twitt_lister(peerid)
  elif category == 'ADD':
    result, text = await adder(mtext, peerid)
    return result, text
  elif category == 'NOTIFED':
    text = notifed(mtext, peerid, id, add=True)
  elif category == 'DELNOTIFED':
    text = notifed(mtext, peerid, id, add=False)
  elif category == 'TESTED':
    text = 'worked'
  elif category == 'DELLTG':
    result, text = delete_tg(mtext, peerid)
  elif category == 'DELLTWITT':
    text = delete_twitt(mtext, peerid)
  elif category == 'ADDPEERID':
    text = add_peerid(mtext, id, admin_id)
  elif category == 'DELLPEERID':
    text = dell_peerid(mtext, id, admin_id)
  elif category == 'LISTPEERIDS':
    text = get_all_peerids(peerid)
  return result, text

async def vk_check(longpoll:VkBotLongPoll):
  values = {
  'act': 'a_check',
  'key': longpoll.key,
  'ts': longpoll.ts,
  'wait': longpoll.wait,
  }
  headers = longpoll.session.headers
  cookies = longpoll.session.cookies
  async with aiohttp.ClientSession(cookies=cookies,headers=headers) as session:
    response = await session.get(url=longpoll.url, params=values, timeout=longpoll.wait + 10)
    response = await response.json()
  if 'failed' not in response:
    longpoll.ts = response['ts']
    return [
        longpoll._parse_event(raw_event)
        for raw_event in response['updates']
    ]
  elif response['failed'] == 1:
      longpoll.ts = response['ts']
  elif response['failed'] == 2:
      longpoll.update_longpoll_server(update_ts=False)
  elif response['failed'] == 3:
      longpoll.update_longpoll_server()
  return []

async def restart_telegram():
  for task in asyncio.all_tasks():
    if task.get_name() == 'TG_WORK':
      task.cancel()
      await asyncio.sleep(3)

def is_added(peerid):
  req = f"SELECT time FROM added_groups_vk WHERE perid_group = {peerid}"
  result = cursor.execute(req).fetchall()
  if len(result) == 0:
    return False
  return True



async def start_vk():
  asyncio.create_task(tg_start(), name='TG_WORK')
  admin_id = int(environ.get("admin_id"))
  while True:
    try:
      group_vk_id = environ.get('group_vk_id')
      longpoll = VkBotLongPoll(vk_session,group_id=group_vk_id)
    except Exception as e:
      ee = f'error on initialization longpool = {e}'
      logging.exception(ee)
      await asyncio.sleep(7)
      continue
    while True:
      try:
        event = await vk_check(longpoll)
        if event == []: continue
        event = event[0]
        if event.type == VkBotEventType.MESSAGE_NEW:
          msg = event.message
          peerid = str(msg.peer_id)
          id = str(msg.from_id)
          if event.message.get('action', 1) != 1:
            if not is_added(peerid) and int(id) == admin_id:
              req = f"INSERT INTO added_groups_vk VALUES ({peerid}, 1, {id}, {msg.get('date')})"
              await send_m(peerid,"Беседа активирована")
            elif not is_added(peerid):
              req = f"INSERT INTO added_groups_vk VALUES ({peerid}, 0, {id}, {msg.get('date')})"
            cursor.execute(req)
            conn.commit()
            continue
          result = False
          if not peerid in get_perids_groups_vk_all() and int(id) != admin_id:
            continue
          mtext = str(msg.text)
          category = select_category(mtext)
          if category:
            result, text = await selected_category(category, peerid, mtext, id, admin_id)
            await send_m(peerid,text)
          else:
            continue
          if result:
            await restart_telegram()
      except Exception as e:
        err = f'error on Vk event: {e}'
        logging.exception(err)
        await asyncio.sleep(7)


async def get_userIds_twitt():
  while True:
    result_twitter = upd_twitter_all()
    if result_twitter == {}:
      await asyncio.sleep(60)
    else:
      return result_twitter


async def send_alert(alert):
  admin_username = environ.get('admin_username_tg')
  client = TelegramClient(path_session1, api_id, api_hash)
  await client.connect()
  await client.send_message(admin_username, alert)
  await client.disconnect()


async def last_post_twitter(session:aiohttp.ClientSession, userId, link):
  shit = {'request': '/1.1/statuses/user_timeline.json', 'error': 'Not authorized.'}
  trying = 0
  while True:
    try:
      url = f'https://api.twitter.com/1.1/statuses/user_timeline.json'
      params = {
        'user_id':int(userId),
        'count':1
      }
      resp = await session.get(url, params = params, )
      assert await resp.json()
      t = await resp.json()
      if trying >= 3:
        text = 'С получением последнего поста '\
              f'твиттера {userId}\n{link}\nкакие то проблемы'
        await send_alert(text)
        return False
      if t == shit:
        trying += 1
        await asyncio.sleep(7)
        continue
      return t
    except (ConnectionResetError, aiohttp.ClientConnectorError, AssertionError):
      await asyncio.sleep(5)
      # t = f'[{link}] ConnectionResetError or AssertionError ...'
      # logging.debug(t) # logging.exception(exc_info=True) 
      continue
    except Exception as e:
      await asyncio.sleep(7)
      # t = f'[{link}] ошибка с получением последнего поста'
      # logging.exception(t)

async def download_photo_tw(session:aiohttp.ClientSession, url_photo, name_photo):
  trying = 0
  while True:
    try:
      resp = await session.get(url_photo)
      photor = await resp.read()
      break
    except:
      await asyncio.sleep(7)
      trying += 1
      if trying >= 3:
        return None
      continue
  path = f'./photo/{name_photo}.jpeg'
  async with aiofiles.open(path, mode='wb') as f:
    await f.write(photor)
  attachment = upload_photo(path)
  os.remove(path)
  owner_id = attachment[0]["owner_id"]
  item_id = attachment[0]["id"]
  attachment = f"photo{owner_id}_{item_id}"
  return attachment

def get_session_tw():
  headers = header.generate()
  bearer = environ.get('bearer')
  headers['authorization'] = bearer ###
  session_tw = aiohttp.ClientSession(headers=headers, trust_env=True)
  return session_tw


async def start_twitter():
  fist_start = True
  sessiontw = get_session_tw()
  while True:
    try:
      result_twitter = await get_userIds_twitt()
      userIds = list(result_twitter.keys())
    except Exception as e:
      logging.exception(f'error on twitter ids {e}')
    for userid in userIds:
      try:
        attachment = None
        lid = result_twitter[userid]['last_post_id']
        twlink = result_twitter[userid]['link']
        lastid_post_bef = 0 if lid is None or lid == '' else int(lid)
        r = await last_post_twitter(sessiontw, userid, twlink)
        if r is False: continue
        if r == []: continue
        last_post = r[0]
        lastid_post_now = last_post['id']
        if lastid_post_bef < lastid_post_now:
          req = f'UPDATE links SET last_post_id = {lastid_post_now} WHERE perid == {userid}'
          cursor.execute(req)
          if fist_start is False:
            user = last_post['user']
            nametw = user['name']
            media_count = user['media_count']
            text = last_post['text']
            if media_count > 0:
              try:
                url_photo = last_post['entities']['media'][0]['media_url_https']
                attachment = await download_photo_tw(sessiontw, url_photo, lastid_post_now)
              except:
                pass
            perids_groups = get_peerids_from_channels(twlink)
            for perid_group in perids_groups:
              t0 = get_notice_from_channel(twlink,perid_group)
              notif_users = '\n'+','.join(map(lambda x:'@id' + x,t0)) if not t0 == [] else ''
              result_text = f'Twitter {twlink}\nName: {nametw}{notif_users}\n\n{text}'
              await send_m(perid_group,result_text,attachment = attachment)
        await asyncio.sleep(7)
      except Exception as e:
        logging.exception(f'error on twitter posts {e}')
        await asyncio.sleep(7)
    fist_start = False
    conn.commit()
    await asyncio.sleep(20)
  

async def main():
  try:
    twitter_work = asyncio.create_task(start_twitter())
    task_vk = asyncio.create_task(start_vk())
    await asyncio.wait([task_vk, twitter_work])
  except Exception as e:
    logging.exception(e)


if __name__ == '__main__':
  try:
    dotenv_file = find_dotenv()
    load_dotenv(dotenv_file)
    api_hash = environ.get('api_hash')
    api_id = environ.get('api_id')
    
    API_VK = environ.get('API_VK')
    vk_session  = vk_api.VkApi(token=API_VK)
    vk = vk_session.get_api()

    path_session,path_session1 = r'./ses.session',r'./ses1.session'
    conn = sql.connect("./data_base.db")
    cursor = conn.cursor()
    header = Headers()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
  except Exception as e:
    logging.exception(e)
