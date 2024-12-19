import random
from aiogram import Bot, Dispatcher, executor, types
from settings import *
from db_mysq import database
from db import datebase
import asyncio, pytz
from datetime import datetime, timedelta
bot = Bot(TOKEN, parse_mode="html")
dp = Dispatcher(bot)
db_mysq = database(host="localhost", user="root", password="", database="puzzlescrusade")
db = datebase()

THRESHOLDS = [
    (timedelta(hours=5), "5h"),
    (timedelta(hours=12), "12h"),
    (timedelta(hours=24), "24h"),
    (timedelta(hours=48), "48h"),
    (timedelta(hours=72), "72h"),
    (timedelta(days=7), "week")
]

filters_options = {
    "level_point": ["1", "2", '3', "4", "5", "6", '7', "8", '9', '10'],
    "coin_balance": ["1000", "<10K", '10K-100K', "100K-1M", "1M-10M", "10M-100M", '100M-10B'],
    "createdAt": ['<24H', '1-3Days', '4-7Days','8-15Days', "16-21Days",'22-30Days','31-60Days','61-90Days','91-120Days', '121-150Days', '151-180Days', "180Days+"],
    "cards": ["0", "1-5", "6-10", "11-15", "16-20", "21-25", "26-30", "31-35", "36-40", "40+"],
    "referrals": ["0", "1-5", "6-10", "11-15", "16-20", "21-30", "31-40", "41-50", "50-100", "100+"]

}
async def post(message: types.Message):
    if message.chat.type == 'private':
        if message.chat.id in admins:
            def combine_markups(markup1, markup2):
                combined_markup = types.InlineKeyboardMarkup(row_width=3)
                for row in markup1.inline_keyboard:
                    combined_markup.inline_keyboard.append(row)

                for row in markup2.inline_keyboard:
                    combined_markup.inline_keyboard.append(row)

                return combined_markup
            await db.add_taking(message.chat.id, "post")
            data = post_data.get(message.chat.id, {})
            buttons = ["Edit text", "Edit captions", "Remove caption", "Add caption",
                       "Attach media", "Remove media",
                      "URL-buttons"]
            if data['media_type'] != "text":
                text_edit = buttons[1]
                if data['text'] == None:
                    text_edit = buttons[3]
            else:
                text_edit = buttons[0]
            markup = types.InlineKeyboardMarkup(row_width=3)
            remove_caption = types.InlineKeyboardButton(text = buttons[2], callback_data="post_rc")
            text_edit = types.InlineKeyboardButton(text = text_edit, callback_data="post_et")
            add_media = types.InlineKeyboardButton(text = "🖼 Attach media", callback_data="post_am")
            url_buttons = types.InlineKeyboardButton(text = "👇 URL-buttons", callback_data="post_ub")
            rurl_buttons = types.InlineKeyboardButton(text = "❌ Remove URL-buttons", callback_data="post_rub")
            filters = types.InlineKeyboardButton(text = "📊 Filters", callback_data="filters")
            post = types.InlineKeyboardButton(text = "Post📤", callback_data="post_post")
            if data['media_type'] != 'text' and data['text'] != None:
                markup.add(remove_caption)
            markup.add(text_edit)
            if data['media_type'] == 'text':
                markup.add(add_media)
            if data['buttons']:
                markup.add(rurl_buttons)
            else:
                markup.row(url_buttons)
            markup.add(filters)
            markup.add(post)
            markup = combine_markups(data['markup'], markup)
            try:
                try:
                    mg = await message.edit_text(text = data['text'], reply_markup=markup, entities=data['entities'])
                except:
                    mg = await message.edit_caption(caption = data['text'], reply_markup=markup, caption_entities=data['entities'])
            except:
                if data['media_type'] == "text":
                    mg = await message.answer(text = data['text'], reply_markup=markup, entities=data['entities'])
                elif data['media_type'] == "photo":
                    mg = await message.answer_photo(caption = data['text'], photo=data['file_id'], reply_markup=markup, caption_entities=data['entities'])
            data['message_id'] = mg.message_id
async def filter_func(message: types.Message):
    if message.chat.type == 'private':
        if message.chat.id in admins:
            def create_button(number, type):
                title = filters_options[type][number]

                if title in filter.get(type):
                    return types.InlineKeyboardButton(f"🟩 {title}",
                                                      callback_data=f'filters{type} {title}')
                else:
                    return types.InlineKeyboardButton(f"☐ {title}", callback_data=f'filters{type} {title}')

            await db.add_taking(message.chat.id, "filters")
            data = post_data[message.chat.id]
            filter = data.get('filters', {})
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("📊 User lvl", callback_data="none")
            )
            markup.row(*[create_button(num, "level_point") for num in range(0, 5)])
            markup.row(*[create_button(num, "level_point") for num in range(5, 10)])
            markup.add(
                types.InlineKeyboardButton("🏦 Balance", callback_data="none")
            )
            markup.row(*[create_button(num, "coin_balance") for num in range(0, 4)])
            markup.row(*[create_button(num, "coin_balance") for num in range(4, 7)])
            # markup.add(
            #     types.InlineKeyboardButton("🌕 Profit Per Hour", callback_data="none")
            # )
            # markup.row(*[create_button(num, "profit_per_hour") for num in range(0, 4)])
            # markup.row(*[create_button(num, "profit_per_hour") for num in range(4, 7)])
            markup.add(
                types.InlineKeyboardButton("🕔 Joining date", callback_data="none")
            )
            markup.row(*[create_button(num, "createdAt") for num in range(0, 4)])
            markup.row(*[create_button(num, "createdAt") for num in range(4, 7)])
            markup.row(*[create_button(num, "createdAt") for num in range(7, 10)])
            markup.row(*[create_button(num, "createdAt") for num in range(10, 12)])
            markup.add(
                types.InlineKeyboardButton("🃏 Cards", callback_data="none")
            )
            markup.row(*[create_button(num, "cards") for num in range(0, 4)])
            markup.row(*[create_button(num, "cards") for num in range(4, 8)])
            markup.row(*[create_button(num, "cards") for num in range(8, 10)])
            markup.add(
                types.InlineKeyboardButton("👥 Referrals", callback_data="none")
            )
            markup.row(*[create_button(num, "referrals") for num in range(0, 4)])
            markup.row(*[create_button(num, "referrals") for num in range(4, 8)])
            markup.row(*[create_button(num, "referrals") for num in range(8, 10)])
            markup.add(
                types.InlineKeyboardButton("↩ Back", callback_data="post")
            )
            try:
                try:
                    await message.edit_text(text=data['text'], reply_markup=markup, entities=data['entities'])
                except:
                    await message.edit_caption(caption=data['text'], reply_markup=markup,
                                                    caption_entities=data['entities'])
            except:
                try:
                    await message.delete()
                except:
                    ...
                if data['media_type'] == "text":
                    await message.answer(text=data['text'], reply_markup=markup, entities=data['entities'])
                elif data['media_type'] == "photo":
                    await message.answer_photo(caption=data['text'], photo=data['file_id'], reply_markup=markup,
                                                    caption_entities=data['entities'])

async def notifications(message: types.Message, time = 0, id= 0):
    if message.chat.type == 'private':
        if message.chat.id in admins:
            await db.add_taking(message.chat.id, "notifications")
            markup = types.InlineKeyboardMarkup(row_width=3)
            data = await db_mysq.get_notifications()
            notf_text = '<b>Notifications\n\nPick the right format</b>'
            if id == 0:
                if time == 0:
                    markup.add(
                        types.InlineKeyboardButton(
                            "🖼️ Pictures", callback_data=f"notfpic_0"
                        ),
                    )
                    markup.add(
                        types.InlineKeyboardButton(
                            "⏲ 5 hours", callback_data=f"notf5h_0"
                        ),
                        types.InlineKeyboardButton(
                            "🕛 12 hours", callback_data=f"notf12h_0"
                        ),
                        types.InlineKeyboardButton(
                            "⏲ 1 Day", callback_data=f"notf24h_0"
                        ),
                        types.InlineKeyboardButton(
                            "⏲ 2 Days", callback_data=f"notf48h_0"
                        ),
                        types.InlineKeyboardButton(
                            "⏲ 3 Day", callback_data=f"notf72h_0"
                        ),
                        types.InlineKeyboardButton(
                            "⏲ Week", callback_data=f"notfweek_0"
                        ),
                        types.InlineKeyboardButton(
                            "🕹️ Button", callback_data=f"notfbutton_0"
                        ),
                    )
                    markup.add(
                        types.InlineKeyboardButton("🔙 Get back", callback_data='admin')
                    )
                else:
                    notf_text = '<b>Notifications\n\nPick the picture/message to remove</b>'
                    i= 0
                    try:
                        for info in data[time]:
                            text = info['text']
                            id = info['id']
                            button_text = f"{text[:40]} ..."
                            if time == "pic":
                                button_text = f"Picture #{id}"
                            markup.add(
                                types.InlineKeyboardButton(
                                    button_text,
                                     callback_data=f"r_m{id}"
                                )
                            )
                            i+=1
                            if i == 50:
                                break
                    except:
                        ...
                    try:
                        lengh = len(data[time])
                    except:
                        lengh = 0
                    if lengh < 50:
                        markup.add(
                            types.InlineKeyboardButton("| + |", callback_data=f'add{time}')
                        )
                    markup.add(
                        types.InlineKeyboardButton("🔙 Get back", callback_data='notf0_0')
                    )
            else:
                ...
            try:
                await message.edit_text(text= notf_text, reply_markup=markup)
            except:
                try:
                    await message.delete()
                except:
                    ...
                await message.answer(text=notf_text, reply_markup=markup)

async def admin(message: types.Message):
    if message.chat.type == 'private':
        if message.chat.id in admins:
            await db.add_hash(message.chat.id, "admin")
            markup = types.InlineKeyboardMarkup(row_width=3)
            notifications = types.InlineKeyboardButton("Notifications", callback_data="notf0_0")
            send = types.InlineKeyboardButton("Broadcast a message", callback_data="broadcast")

            markup.add(notifications, send)
            text = '<b>Admin panel:</b>'
            try:
                await message.edit_text(text=text, reply_markup=markup)
            except:
                try:
                    await message.delete()
                except:
                    ...
                await message.answer(text=text, reply_markup=markup)
@dp.message_handler(commands=['chatid'])
async def chatid(message: types.Message):
    try:
        await bot.send_message(message.chat.id, f"<b>🪪 Your chat id:</b> <code>{message.chat.id}</code>")
    except:
        await bot.send_message(message.chat.id, "Error")
@dp.message_handler(commands=['admin'])
async def admin_command(message: types.Message):
    return await admin(message)
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    if await db.user_exist(message.chat.id) is False:
        await db.add_user(message.chat.id)
    markup = types.InlineKeyboardMarkup(row_width=1).add(
        types.InlineKeyboardButton("PLAY", url = "https://t.me/pctabot/pcta"),
        types.InlineKeyboardButton("Join Our Chat", url="https://t.me/puzzlescrusade"),
    )
    await message.answer_photo(
        photo="AgACAgIAAxkDAALUDWdi_klxDNBmN8-HqDrvUEmmzAABBgACM-UxGwUoGUt1f1BiFAVgqQEAAwIAA3kAAzYE",
        caption=
        "GM, Crusader!\n\n"
        "<b>Tap, fulfill missions, and unlock hero cards.</b>"
        " Upgrade them to access battle mode for epic fights in Wallachia. Level up and claim exclusive early-bird prizes,"
        " giveaways, and NFTs. Plus, an <b>Epic Airdrop</b> of our <b>Crypto Coin</b> is coming. Go play!",
        reply_markup=markup
    )

@dp.message_handler(content_types=["text", "photo", "document", "video", "voice",'video_note'])
async def partTwo(message: types.Message):
    action = await db.taking_exist(message.chat.id)
    if action == "add_notification":
        if message.content_type == "text":
            await db_mysq.add_notification(await db.hash_exist(message.chat.id), message.text)
        else:
            await db_mysq.add_notification(await db.hash_exist(message.chat.id), message.photo[0].file_id)
        return await notifications(message, time=await db.hash_exist(message.chat.id))
    if action == "post_text_edit":
        post_data[message.chat.id]['entities'] = message.entities
        post_data[message.chat.id]['text'] = message.text
        return await post(message)
    elif action == "media_receiver":
        post_data[message.chat.id]["media_type"] = message.content_type
        if message.content_type == "photo":
            post_data[message.chat.id]['file_id'] = message.photo[0].file_id
        return await post(message)
    elif action == "buttons_receiver":
        try:
            text = message.text
            markup = types.InlineKeyboardMarkup(row_width=3)
            lines = text.strip().split('\n')
            for line in lines:
                buttons = []
                for button_data in line.split('|'):
                    button_text, button_url = button_data.split(' - ')
                    button_text = button_text.strip()
                    button_url = button_url.strip()
                    button = types.InlineKeyboardButton(text=button_text, url=button_url)
                    buttons.append(button)
                markup.row(*buttons)
            post_data[message.chat.id]['markup'] = markup
            post_data[message.chat.id]['buttons'] = True
            return await post(message)
        except:
            post_data[message.chat.id]['markup'] = types.InlineKeyboardMarkup(row_width=3)
            post_data[message.chat.id]['buttons'] = False
            return await message.answer(
                "Impossible to send this keyboard. Follow the format specified in the previous message.",
                reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                    types.InlineKeyboardButton("Back", callback_data="post")
                ))
    elif action == "broadcast":
        if post_data.get(message.chat.id, {}).get("message_id", None) != None:
            try:
                await bot.delete_message(chat_id=message.chat.id,
                                         message_id=post_data.get(message.chat.id, {}).get("message_id", None))
            except:
                ...
        post_data[message.chat.id] = {
            "text": None,
            "media_type": message.content_type,
            "file_id": None,
            "entities": message.entities,
            "message_id": int,
            "markup": types.InlineKeyboardMarkup(row_width=3),
            "buttons": False,
            "filters": {
                "level_point": [],
                "coin_balance": [],
                # "profit_per_hour": [],
                "createdAt": [],
                "cards": [],
                "referrals": []
            }
        }
        if message.content_type == "text":
            post_data[message.chat.id]['text'] = message.text
        elif message.content_type == "photo":
            post_data[message.chat.id]['entities'] = message.caption_entities
            post_data[message.chat.id]['text'] = message.caption
            post_data[message.chat.id]['file_id'] = message.photo[0].file_id
        return await post(message)
@dp.callback_query_handler()
async def callback_inline(call: types.CallbackQuery):
    if call.message.chat.type == 'private':
        if call.message.chat.id in admins:
            if call.data[:4] == 'notf':
                info = call.data[4:].split("_")
                try:
                    key = int(info[0])
                except:
                    key = info[0]
                return await notifications(call.message, key, int(info[1]))
            elif call.data[:3] == 'add':
                await db.add_hash(call.message.chat.id, call.data[3:])
                await db.add_taking(call.message.chat.id, "add_notification")
                await call.message.edit_text(
                    f"Send new notification for {call.data[3:]} here",
                    reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                        types.InlineKeyboardButton('🔙 Get back', callback_data=f"notf{call.data[3:]}_0")
                    )
                )
            elif call.data[:3] == "r_m":
                data = await db_mysq.get_notification(call.data[3:])
                text = data['text']
                ttime = data['time']
                id = data['id']
                if ttime == 'pic':
                    try:
                        await call.message.delete()
                    except:
                        ...
                    await call.message.answer_photo(
                        photo=text,
                        caption = f"<b>Do you want to remove this picture?</b>",
                        reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                            types.InlineKeyboardButton('✅ Yes', callback_data=f"rem{id}"),
                            types.InlineKeyboardButton('❌ No', callback_data=f"notf{ttime}_0"),
                            types.InlineKeyboardButton('🔙 Get back', callback_data=f"notf{ttime}_0")
                        )
                    )
                else:
                    await call.message.edit_text(
                        f"<b>Do you want to remove this message?:\n\n"
                        f"{text}</b>",
                        reply_markup=types.InlineKeyboardMarkup(row_width=2).add(
                            types.InlineKeyboardButton('✅ Yes', callback_data=f"rem{id}"),
                            types.InlineKeyboardButton('❌ No', callback_data=f"notf{ttime}_0"),
                            types.InlineKeyboardButton('🔙 Get back', callback_data=f"notf{ttime}_0")
                        )
                    )
            elif call.data[:3] == "rem":
                data = await db_mysq.get_notification(call.data[3:])
                ttime = data['time']
                await db_mysq.rem_notification(call.data[3:])
                return await notifications(call.message, time=ttime)
            elif call.data == 'broadcast':
                await db.add_taking(call.message.chat.id, "broadcast")
                await call.message.edit_text(
                    text = "Send message that you want to broad cast to users:",
                    reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                        types.InlineKeyboardButton('🔙 Get back', callback_data=f"admin"))
                    )
            elif call.data[:4] == "post":
                data = post_data.get(call.message.chat.id, {})
                option = call.data[5:]

                if option == "et":
                    if post_data[call.message.chat.id]['media_type'] != "text":
                        try:
                            await call.message.delete()
                        except:
                            ...
                    await db.add_taking(call.message.chat.id, "post_text_edit")
                    try:
                        await call.message.answer(data['text'], entities=data['entities'])
                    except:
                        ...
                    return await call.message.answer("Send the new text to the bot here",
                                                     reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                                                         types.InlineKeyboardButton("Back", callback_data="post")
                                                     ))
                elif option == "rc":
                    post_data[call.message.chat.id]['text'] = None
                    post_data[call.message.chat.id]['entities'] = []
                    return await post(call.message)
                elif option == "am":
                    try:
                        await call.message.delete()
                    except:
                        ...
                    await db.add_taking(call.message.chat.id, "media_receiver")
                    return await call.message.answer("Send the new media file to the bot here",
                                                     reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                                                         types.InlineKeyboardButton("Back", callback_data="post")
                                                     ))
                elif option == "ub":
                    await db.add_taking(call.message.chat.id, "buttons_receiver")
                    return await call.message.answer("Send a list of URL buttons to the bot in the following format:\n\n"
                                                     "<code>Button 1 - http://example1.com\n"
                                                     "Button 2 - http://example2.com</code>\n\n"
                                                     "Use separator \"|\" to add up to three buttons into one row (max. 15 rows):\n\n\n"
                                                     "<code>Button 1 - http://example1.com | Button 2 - http://example2.com\n"
                                                     "Button 3 - http://example3.com | Button 4 - http://example4.com</code>",
                                                     reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                                                         types.InlineKeyboardButton("Back", callback_data="post")
                                                     ))
                elif option == "rub":
                    post_data[call.message.chat.id]['markup'] = types.InlineKeyboardMarkup(row_width=3)
                    post_data[call.message.chat.id]['buttons'] = False
                    return await post(call.message)
                elif option == "post":
                    data = post_data[call.message.chat.id]
                    try:
                        await call.message.delete()
                    except:
                        ...
                    await bot.answer_callback_query(call.id, "Sending...", show_alert=True)
                    await broadcast(call.message, post_data[call.message.chat.id])
                    post_data[call.message.chat.id] = {}
                    return
                return await post(call.message)
            elif call.data[:7] == "filters":
                if call.data[7:] != "":
                    call_data = call.data[7:].split(" ")
                    data = post_data[call.message.chat.id].get('filters', {})
                    data = data[call_data[0]]

                    if call_data[1] not in data:
                        data.append(call_data[1])
                    else:
                        data.remove(call_data[1])
                return await filter_func(call.message)
            elif call.data == "admin":
                return await admin(call.message)
async def broadcast(message: types.Message, data):
    users = await db_mysq.get_users_by_filters(data['filters'])
    await bot.send_message(message.chat.id, "<b>Start broadcasting\n"
                                            f"Found {len(users)} users that match the filters</b>")
    stat = await bot.send_message(message.chat.id, f"<b>🎙Sent messages: 0/{len(users)}\n"
                                                   f"✅Successful: 0\n"
                                                   f"❌Failed: 0</b>")
    i = 0
    s = 0
    f = 0
    for user in users:
        try:
            if data['media_type'] == "text":
                await bot.send_message(chat_id=user['t_user_id'], text=data['text'],
                                            reply_markup=data['markup'], entities=data['entities'])
            elif data['media_type'] == "photo":
                await bot.send_photo(chat_id=user['t_user_id'], caption=data['text'],
                                          photo=data['file_id'], reply_markup=data['markup'],
                                          caption_entities=data['entities'])
            s+=1
        except Exception as ex:
            print(ex)
            f+=1
            ...
        i+=1
        if i%10 == 0:
            await stat.edit_text(text= f"<b>🎙Send messages: {i}/{len(users)}\n"
                                                    f"✅Successful: {s}\n"
                                                    f"❌Failed: {f}</b>")
    await stat.edit_text(text=f"<b>🎙Sent messages: {i}/{len(users)}\n"
                              f"✅Successful: {s}\n"
                              f"❌Failed: {f}</b>")
async def send_notification(user_id, message, button, photo = False):
    try:
        if photo:
            await bot.send_photo(user_id, photo= photo, caption=message, parse_mode="html",
                                   reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                                       types.InlineKeyboardButton(button, url="https://t.me/pctabot/pcta")
                                   ))
        else:
            await bot.send_message(user_id, text = message, parse_mode="html",
                                   reply_markup=types.InlineKeyboardMarkup(row_width=1).add(
                types.InlineKeyboardButton(button, url = "https://t.me/pctabot/pcta")
            ))
    except Exception as e:
        print(f"Error sending notification to {user_id}: {e}")

def get_time_diff(last_activity):
    last_activity = last_activity.replace(tzinfo=pytz.utc)
    now = datetime.now(pytz.utc)
    time_diff = now - last_activity
    return time_diff
async def check_users():
    users = await db_mysq.get_all_users_activity()
    if users:
        current_time = datetime.now(pytz.utc)
        notifications = await db_mysq.get_notifications(without_id=True)

        for user in users:
            user_id = user['t_user_id']
            last_activity = user['updatedAt']
            last_notified = user.get('last_notified')
            if isinstance(last_activity, str):
                last_activity = datetime.strptime(last_activity, '%Y-%m-%d %H:%M:%S')
            last_activity = last_activity.replace(tzinfo=pytz.utc)

            if last_notified and last_notified.tzinfo is None:
                last_notified = last_notified.replace(tzinfo=pytz.utc)
            for threshold, key in THRESHOLDS:
                threshold_time = last_activity + threshold

                threshold_time = threshold_time.replace(tzinfo=pytz.utc)

                if current_time >= threshold_time:
                    if not last_notified or last_notified < threshold_time:
                        message = random.choice(notifications[key])
                        button = random.choice(notifications["button"])
                        if random.randint(1, 5) == 3:
                            photo = False
                        else:
                            photo = random.choice(notifications['pic'])
                        await send_notification(user_id, message, button, photo)
                        await db_mysq.update_last_notified(user_id)
                        break
async def periodic_check():
    while True:
        await check_users()
        await asyncio.sleep(60*60)

if __name__ == '__main__':
    from aiogram import executor
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_check())
    executor.start_polling(dp, loop=loop, skip_updates=True)















