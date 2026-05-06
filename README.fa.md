# WalrusHF

[English](README.md)

WalrusHF یک ربات تلگرام را داخل Hugging Face Space اجرا می‌کند و فایل‌های دریافتی را در روبیکا آپلود می‌کند. تلگرام پنل اصلی کنترل است؛ صفحه Space فقط داشبورد زنده برای وضعیت پردازش‌ها، صف، ذخیره‌سازی و لاگ‌هاست.

نام Walrus از سریال Black Sails الهام گرفته شده است: کشتی Captain Flint به نام Walrus.

<img width="3456" height="3228" alt="587988794-00199bb8-1882-4ec5-b6cf-b38c8b57d14c" src="https://github.com/user-attachments/assets/274006d1-8de8-4402-83bc-3284cfa50690" />

## امکانات

- دریافت فایل از تلگرام و لینک مستقیم `http://` یا `https://`
- صف کردن لینک مستقیم فایل از داشبورد Space بدون استفاده از تلگرام
- دانلود فایل داخل محیط Space
- صف‌بندی آپلودها تا انتقال‌های روبیکا هم‌زمان اجرا نشوند
- آپلود در Saved Messages روبیکا یا یک کانال روبیکا
- نمایش پیشرفت دانلود، صف، آپلود، تلاش مجدد و خطا در تلگرام
- پشتیبانی از لغو، پاک‌سازی، تلاش مجدد، تلاش مجدد همه فایل‌ها و ورود به روبیکا

## نصب

<img width="1536" height="1024" alt="Generated image 1" src="https://github.com/user-attachments/assets/45187c3e-650a-4435-b9a4-4cd1bf44d4f3" />

### 1. ساخت Hugging Face Space

صفحه https://huggingface.co/spaces را باز کنید و یک Space جدید با این تنظیمات بسازید:

- **Space SDK:** `Gradio`
- **Gradio template:** `Blank`
- **Hardware:** `CPU Basic`
- **Visibility:** بهتر است `Private` باشد
- **Space name:** هر نامی، مثلا `walrushf`

### 2. اضافه کردن Storage Bucket

در همان صفحه ساخت Space، گزینه **Mount a bucket to this Space** را فعال کنید:

- **Bucket:** یک bucket خصوصی جدید بسازید، یا یک bucket خصوصی موجود را mount کنید
- **Mount path:** `/data`
- **Access mode:** `Read & Write`

WalrusHF فایل‌های session، دانلودها و اطلاعات صف را در `/data/walrus` نگه می‌دارد. اگر `/data` پایدار نباشد، برنامه از `/tmp/walrus` استفاده می‌کند و این مسیر ممکن است بعد از ری‌استارت Space پاک شود.

### 3. Deploy از GitHub

این ریپو را clone کنید و روی Space خودتان push کنید:

```bash
git clone git@github.com:rezaaa/WalrusHF.git
cd WalrusHF
git remote add space https://huggingface.co/spaces/USERNAME/SPACE_NAME
git push space main:main
```

به جای `USERNAME/SPACE_NAME` مسیر Space خودتان را بگذارید.

اگر Space فایل‌های اولیه دارد و push رد شد:

```bash
git push --force-with-lease space main:main
```

### 4. تکمیل راه‌اندازی

1. Secretهای لازم را در تنظیمات Space اضافه کنید.
2. Space را restart کنید.
3. ربات تلگرام خودتان را باز کنید و `/start` بفرستید.

Hugging Face فایل [app.py](app.py) را اجرا می‌کند. این فایل ربات تلگرام، worker آپلود روبیکا و داشبورد روی پورت `7860` را بالا می‌آورد.

## Secretهای لازم

این مقدارها را در **Space settings -> Variables and secrets -> Secrets** اضافه کنید:

```env
API_ID=123456
API_HASH=your_telegram_api_hash
BOT_TOKEN=123456:your_bot_token
OWNER_TELEGRAM_ID=123456789
```

از کجا بگیرید:

- `API_ID` و `API_HASH`: از https://my.telegram.org
- `BOT_TOKEN`: از BotFather در تلگرام
- `OWNER_TELEGRAM_ID`: آیدی عددی اکانت تلگرام شما

`OWNER_TELEGRAM_ID` بسیار مهم است. اگر خالی یا اشتباه باشد، هر کسی که به ربات پیام بدهد می‌تواند از آن استفاده کند.

## متغیرهای اختیاری

فقط اگر می‌خواهید مقدارهای پیش‌فرض را تغییر دهید این‌ها را اضافه کنید:

```env
TELEGRAM_SESSION=walrus
RUBIKA_SESSION=rubika_session
RUBIKA_TARGET=me
RUBIKA_TARGET_TITLE=Saved Messages
WALRUS_MAX_FILE_BYTES=8589934592
WALRUS_MIN_FREE_BYTES=536870912
```

نکته‌ها:

- `RUBIKA_TARGET=me` فایل‌ها را در Saved Messages روبیکا آپلود می‌کند.
- مقدار پیش‌فرض `WALRUS_MAX_FILE_BYTES` برابر 8 GiB است.
- با `WALRUS_MAX_FILE_BYTES=0` محدودیت حجم داخلی برنامه غیرفعال می‌شود.
- دانلود فایل از طریق ربات تلگرام به محدودیت تلگرام وابسته است و حداکثر 2 GB برای هر فایل است. برای فایل‌های مستقیم بزرگ‌تر، از فرم URL در داشبورد Space استفاده کنید.
- لینک‌های `file://` به صورت پیش‌فرض غیرفعال هستند. فقط اگر ریسک آن را می‌دانید فعالشان کنید:

```env
WALRUS_ALLOW_FILE_URLS=true
```

## ورود به روبیکا

ساده‌ترین روش از داخل تلگرام است:

1. Space را روشن کنید.
2. ربات تلگرام را باز کنید.
3. دستور `/start` را بفرستید.
4. اگر session روبیکا وجود نداشته باشد، WalrusHF شماره تلفن روبیکا را می‌پرسد.
5. کد OTP یا رمز را وقتی ربات درخواست کرد ارسال کنید.

بعد از ورود، session روبیکا در مسیر `/data/walrus/sessions` ذخیره می‌شود.

## دستورهای ربات

- `/start` - باز کردن منوی اصلی یا راه‌اندازی
- `/settings` - نمایش اکانت روبیکا و مقصد آپلود
- `/set_rubika` - شروع ورود به روبیکا
- `/status` - نمایش صف، انتقال‌های فعال، خطاها و وضعیت storage
- `/transfers` - نمایش انتقال‌های فعال، در صف و قابل تلاش مجدد
- `/cleanup` - پیش‌نمایش فایل‌های قابل پاک‌سازی، وضعیت stale آپلود و رکوردهای failed مرده
- `/cleanup confirm` - پاک کردن موارد امن و clear کردن وضعیت stale
- `/cancel` - نمایش دکمه‌های لغو
- `/retry <task_id>` - تلاش مجدد برای یک انتقال ناموفق
- `/retry_all` - تلاش مجدد برای همه انتقال‌های قابل تکرار

## داشبورد

صفحه Space هر 2 ثانیه به‌روزرسانی می‌شود. endpointهای مفید:

```text
/health
/status.json
```

موارد مهم در داشبورد:

- `Telegram bot: running` یعنی پردازش ربات تلگرام فعال است.
- `Rubika worker: running` یعنی worker آپلود روبیکا فعال است.
- `Config: ok` یعنی Secretهای لازم وجود دارند.
- `Queue` کارهای در صف را نشان می‌دهد.
- `Active upload` آپلود فعال فعلی را نشان می‌دهد.

در داشبورد هم می‌توانید یک لینک مستقیم `http://` یا `https://` وارد کنید. WalrusHF فایل را داخل Space دانلود می‌کند، آن را برای آپلود روبیکا در صف می‌گذارد و وضعیت دانلود/آپلود را در صفحه وب نشان می‌دهد. انتقال‌های URL داشبورد از همان صفحه قابل cancel هستند و موارد completed/failed/cancelled با دکمه **Clear Done** پاک می‌شوند. این مسیر از تلگرام جداست و پیام وضعیت تلگرام ارسال نمی‌کند.

## ربات اختیاری دانلود ویدئوی یوتیوب در تلگرام

اگر می‌خواهید لینک ویدئوهای یوتیوب را قبل از ارسال به WalrusHF به فایل تلگرام تبدیل کنید، این ربات شخص ثالث می‌تواند مفید باشد:

- [@allsaverbot](https://t.me/allsaverbot) - تبدیل لینک ویدئوهای یوتیوب به فایل قابل دانلود در تلگرام

این ربات بخشی از WalrusHF نیست و ممکن است تغییر کند، از کار بیفتد یا محدودیت‌های خودش را داشته باشد. فقط برای محتوایی از آن استفاده کنید که اجازه دانلود و اشتراک‌گذاری آن را دارید.

## رفع مشکل

اگر ربات جواب نمی‌دهد:

- لاگ‌های Space را بررسی کنید.
- مطمئن شوید `API_ID`، `API_HASH` و `BOT_TOKEN` مقدار واقعی دارند، نه placeholder.
- مطمئن شوید `OWNER_TELEGRAM_ID` عددی است.
- بعد از تغییر Secretها، Space را restart کنید.

اگر آپلودها در صف می‌مانند:

- لاگ‌های داشبورد را برای `Rubika worker` بررسی کنید.
- دستور `/transfers` را در تلگرام اجرا کنید.
- مطمئن شوید ورود به روبیکا با `/start` یا `/set_rubika` کامل شده است.
- مطمئن شوید bucket یا persistent storage روی `/data` mount شده است.

اگر پیام اصلی پیشرفت آپدیت نمی‌شود:

- Rubika worker رویدادهای پیشرفت را به صورت محلی می‌نویسد.
- ربات تلگرام آن رویدادها را با Pyrogram اعمال می‌کند.
- لاگ‌ها را برای `Telegram event bridge failed` بررسی کنید.

## تست محلی

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

سپس باز کنید:

```text
http://localhost:7860
```

## هشدار

این پروژه برای workflowهای شخصی انتقال فایل، تحقیق و آزمایش است. از آن برای اسپم، سوءاستفاده، دسترسی غیرمجاز، نقض حریم خصوصی یا کار غیرقانونی استفاده نکنید. مسئولیت رعایت قوانین پلتفرم‌ها، قوانین محلی و حقوق دیگران با شماست.
