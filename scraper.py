


# import asyncio
# import json
# from datetime import datetime
# from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
# from bs4 import BeautifulSoup
# from openai import OpenAI
# import re
# import gspread
# from google.oauth2.service_account import Credentials
# import os
# from typing import List, Dict
# import hashlib
# import pytz
# import time


# # ============================================================
# # Custom Exceptions
# # ============================================================
# class CreditExhaustedException(Exception):
#     pass


# # ============================================================
# # Perplexity Client
# # ============================================================
# perplexity_client = OpenAI(
#     api_key=os.environ.get("PERPLEXITY_API_KEY"),
#     base_url="https://api.perplexity.ai"
# )

# ANALYSIS_MODEL       = "sonar-pro"
# SCRIPT_MODEL         = "sonar-reasoning-pro"

# ANALYSIS_INPUT_COST  = 1.0 / 1_000_000
# ANALYSIS_OUTPUT_COST = 1.0 / 1_000_000
# SCRIPT_INPUT_COST    = 2.0 / 1_000_000
# SCRIPT_OUTPUT_COST   = 8.0 / 1_000_000

# IST = pytz.timezone('Asia/Kolkata')


# # ============================================================
# # Config
# # ============================================================
# # GOOGLE_SHEETS_CREDENTIALS_FILE = "credentials.json"
# # GOOGLE_SHEET_NAME               = "Salaam Thane Scripts"
# # GOOGLE_WORKSHEET_NAME           = "Scripts"
# GOOGLE_SHEETS_CREDENTIALS_FILE = "credentials.json"
# GOOGLE_SHEET_NAME               = "Salam Thane"
# GOOGLE_WORKSHEET_NAME           = "Sheet1"
# TARGET_SCRIPTS = 20

# VALID_CATEGORIES = [
#     "crime", "environment", "development", "local_events",
#     "health", "achievement", "politics", "general"
# ]

# # ✅ Dynamic CTA — different per category (confirmed from real audio analysis)
# CATEGORY_CTA = {
#     "crime":        "या प्रकरणावर तुम्हाला काय वाटतं? कमेंट्स मध्ये नक्की सांगा आणि अशाच महत्त्वाच्या अपडेटसाठी फॉलो करा सलाम ठाणे.",
#     "environment":  "तुम्हाला काय वाटतं? कमेंट्स मध्ये नक्की सांगा आणि अशाच महत्त्वाच्या अपडेटसाठी फॉलो करा सलाम ठाणे.",
#     "development":  "तुमचं या विकासकामाबद्दल काय मत आहे? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे.",
#     "local_events": "तुम्ही यात सहभागी होणार का? कमेंट्स मध्ये सांगा आणि अशाच झन्नाट अपडेट साठी फॉलो करा सलाम ठाणे.",
#     "health":       "तुम्हाला हा उपक्रम कसा वाटला? कमेंट्स मध्ये सांगा आणि अशाच भन्नाट अपडेटसाठी फॉलो करा सलाम ठाणे.",
#     "achievement":  "या ऐतिहासिक क्षणाबद्दल तुमचं काय मत आहे? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे.",
#     "politics":     "या राजकारणाबद्दल तुम्हाला काय वाटतं? कमेंट्स मध्ये नक्की सांगा आणि अशाच महत्त्वाच्या अपडेटसाठी फॉलो करा सलाम ठाणे.",
#     "general":      "तुम्हाला काय वाटतं? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे.",
# }
# DEFAULT_CTA = "तुम्हाला काय वाटतं? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे."

# REFUSAL_KEYWORDS = [
#     "I appreciate", "I should clarify", "I'm Perplexity",
#     "search assistant", "I'm not able", "I cannot create",
#     "Would you like", "clarify my role", "I'm an AI",
#     "as an AI", "I don't create",
#     "मुझे खेद है", "मैं इस अनुरोध", "खोज परिणामों में",
#     "प्रदान किए गए", "कृपया स्पष्ट करें", "मैं सही तरीके",
#     "विशिष्ट तथ्य नहीं", "आवश्यक माहिती",
#     "मला खेद आहे", "मला क्षमस्व", "उत्तर देण्यासाठी आवश्यक",
#     "शोध परिणामांमध्ये", "कृपया एक पूर्ण बातमी",
#     "अधिक संबंधित शोध", "विशिष्ट घटना", "तपशील पुनः तपास",
#     "मी Perplexity", "मी perplexity", "माझी भूमिका",
#     "मूळ कार्याच्या विरुद्ध", "script लिहिण्याची विनंती",
#     "सूचना देणे", "संशोधित उत्तरे", "मी एक AI",
#     "script writer नाही", "मी तयार करू शकत नाही",
#     "शोध निकालांमध्ये", "मेल होत नाही", "script तयार करू शकतो पण",
#     "विस्तृत search results", "स्पष्ट करा"
# ]

# SKIP_TITLE_KEYWORDS = [
#     'राशीभविष्य', 'राशिभविष्य', 'ज्योतिष', 'पूजा', 'अध्यात्म',
#     'horoscope', 'rashifal', 'astrology', 'dharm', 'puja',
#     'utility', 'यूटिलिटी', 'आध्यात्मिक', 'spirituality',
#     'धार्मिक परंपरा', 'मंदिर', 'व्रत', 'उपवास', 'rashibhavishya',
#     'धार्मिक', 'ज्योतिष', 'recipe', 'cooking', 'फॅशन',
# ]

# SKIP_CONTENT_KEYWORDS = [
#     'tv9 मराठी एक 24/7 मराठी भाषिक वृत्तवाहिनी',
#     'अध्यात्म बातम्यांचा विशेष विभाग आहे जो',
#     'राशीभविष्य, मंदिरातील पूजा, धार्मिक परंपरा',
#     'आध्यात्मिक जीवनाची संपूर्ण माहिती',
#     'यूटिलिटी बातम्या म्हणजे काय',
#     'utility news definition'
# ]

# # ✅ Thane district keywords — for filtering Thane-relevant content
# THANE_KEYWORDS = [
#     'thane', 'ठाण', 'dombivli', 'डोंबिवली', 'kalyan', 'कल्याण',
#     'bhiwandi', 'भिवंडी', 'ulhasnagar', 'उल्हासनगर',
#     'mira-road', 'miraroad', 'मीरा रोड', 'ambernath', 'अंबरनाथ',
#     'badlapur', 'बदलापूर', 'titwala', 'टिटवाळा',
#     'shahad', 'vithalwadi', 'kdmc', 'tmc', 'kopri', 'mumbra',
#     'wagle', 'varsova', 'majiwada', 'kasarwadavali', 'palghar'
# ]

# SKIP_URL_PATTERNS = [
#     'javascript:', 'mailto:', '#',
#     '/category/', '/tag/', '/author/',
#     'facebook.com', 'twitter.com', 'instagram.com',
#     'youtube.com', 'whatsapp.com', '/myaccount/',
#     '/install_app', '/advertisement', '/epaper',
#     'web-stories', 'photo-gallery', '/videos/',
#     '/games/', '/jokes/', '/terms-and-conditions',
#     '/topic/', '/widget/', '/livetv',
#     'articlelist', '/live',
#     '/utility/', '/utilities/',
#     '/adhyatma/', '/astrology/', '/rashifal/',
#     '/horoscope/', '/jyotish/', '/puja/',
#     '/dharm/', '/dharma/', '/spirituality/',
#     '/rashibhavishya/', '/religion/',
#     '/north', '/south', '/west', '/east',
#     '/nashik', '/aurangabad', '/nagpur', '/konkan',
#     '/vidarbha', '/marathwada',
#     '/pune', '/solapur', '/kolhapur', '/satara',
# ]


# # ============================================================
# # Token Tracking
# # ============================================================
# total_input_tokens  = 0
# total_output_tokens = 0
# total_cost          = 0.0
# processed_hashes    = set()


# # ============================================================
# # News Sites — Thane-Specific Sources (5+4+4+4+3 = 20)
# # ============================================================
# NEWS_SITES = [
#     {
#         "name": "Maharashtra Times Thane",
#         "url": "https://maharashtratimes.com/maharashtra/thane-news",
#         "link_pattern": "maharashtratimes.com",
#         "thane_strict": False,
#         "target": 5,
#         "fetch_limit": 35
#     },
#     {
#         "name": "Lokmat Thane",
#         "url": "https://www.lokmat.com/thane/",
#         "link_pattern": "lokmat.com",
#         "thane_strict": False,
#         "target": 4,
#         "fetch_limit": 30
#     },
#     {
#         "name": "ABP Majha Thane",
#         "url": "https://marathi.abplive.com/district/thane",
#         "link_pattern": "abplive.com",
#         "thane_strict": False,
#         "target": 4,
#         "fetch_limit": 30
#     },
#     {
#         "name": "TV9 Marathi Thane",
#         "url": "https://www.tv9marathi.com/city/thane",
#         "link_pattern": "tv9marathi.com",
#         "thane_strict": False,
#         "target": 4,
#         "fetch_limit": 30
#     },
#     {
#         "name": "Navshakti Thane",
#         "url": "https://www.navshakti.co.in/thane/",
#         "link_pattern": "navshakti.co.in",
#         "thane_strict": False,
#         "target": 3,
#         "fetch_limit": 20
#     },
# ]


# # ============================================================
# # Google Sheets Setup — with 503 retry logic
# # ============================================================
# def setup_google_sheets(max_retries: int = 5, retry_delay: int = 10):
#     for attempt in range(1, max_retries + 1):
#         try:
#             scope = [
#                 'https://spreadsheets.google.com/feeds',
#                 'https://www.googleapis.com/auth/drive'
#             ]
#             creds = Credentials.from_service_account_file(
#                 GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=scope
#             )
#             client = gspread.authorize(creds)

#             try:
#                 sheet = client.open(GOOGLE_SHEET_NAME)
#                 print(f"✅ Connected: '{GOOGLE_SHEET_NAME}'")
#             except gspread.SpreadsheetNotFound:
#                 sheet = client.create(GOOGLE_SHEET_NAME)
#                 print(f"✅ Created: '{GOOGLE_SHEET_NAME}'")

#             try:
#                 worksheet = sheet.worksheet(GOOGLE_WORKSHEET_NAME)
#                 print(f"✅ Worksheet: '{GOOGLE_WORKSHEET_NAME}'")
#                 current_rows = worksheet.row_count
#                 if current_rows < 2000:
#                     worksheet.add_rows(5000 - current_rows)
#                     print(f"✅ Expanded sheet: {current_rows} → 5000 rows")
#                 else:
#                     print(f"✅ Sheet has {current_rows} rows — OK")

#             except gspread.WorksheetNotFound:
#                 worksheet = sheet.add_worksheet(
#                     title=GOOGLE_WORKSHEET_NAME, rows=5000, cols=10
#                 )
#                 worksheet.update('A1:E1', [[
#                     'Timestamp (IST)', 'Category', 'Title', 'Script', 'Source Link'
#                 ]])
#                 # ✅ Salaam Thane green header
#                 worksheet.format('A1:E1', {
#                     'textFormat': {
#                         'bold': True,
#                         'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
#                     },
#                     'backgroundColor': {'red': 0.0, 'green': 0.5, 'blue': 0.25},
#                     'horizontalAlignment': 'CENTER'
#                 })
#                 worksheet.set_column_width('A', 200)
#                 worksheet.set_column_width('B', 150)
#                 worksheet.set_column_width('C', 400)
#                 worksheet.set_column_width('D', 600)
#                 worksheet.set_column_width('E', 400)
#                 print(f"✅ Created worksheet with headers")

#             return worksheet

#         except gspread.exceptions.APIError as e:
#             error_str = str(e)
#             if any(code in error_str for code in ['503', '500', '429', '502']):
#                 if attempt < max_retries:
#                     print(f"⚠️ Google Sheets {error_str[:20]} (attempt {attempt}/{max_retries}) — retrying in {retry_delay}s...")
#                     time.sleep(retry_delay)
#                     continue
#                 else:
#                     print(f"❌ Google Sheets unavailable after {max_retries} attempts: {e}")
#                     return None
#             else:
#                 print(f"❌ Sheets API error (non-retryable): {e}")
#                 return None
#         except FileNotFoundError:
#             print(f"❌ credentials.json not found!")
#             return None
#         except Exception as e:
#             print(f"❌ Sheets setup error: {e}")
#             import traceback
#             traceback.print_exc()
#             return None


# # ============================================================
# # Save to Google Sheets — with retry
# # ============================================================
# def save_to_google_sheets(worksheet, category, title, script, source_link, max_retries: int = 3):
#     for attempt in range(1, max_retries + 1):
#         try:
#             timestamp = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')

#             script = '\n'.join(str(i) for i in script) if isinstance(script, list) else str(script).strip()
#             script = script.replace('[', '').replace(']', '')
#             title = str(title).strip()
#             source_link = str(source_link).strip()
#             category = str(category).strip().lower()

#             if category not in VALID_CATEGORIES:
#                 category = "general"

#             next_row = len(worksheet.get_all_values()) + 1
#             worksheet.append_row(
#                 [timestamp, category, title, script, source_link],
#                 value_input_option='RAW'
#             )

#             worksheet.format(f'A{next_row}:E{next_row}', {
#                 'textFormat': {
#                     'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
#                     'fontSize': 10
#                 },
#                 'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
#                 'wrapStrategy': 'WRAP',
#                 'verticalAlignment': 'TOP'
#             })

#             # ✅ Salaam Thane category colour palette
#             category_colors = {
#                 'crime':        {'red': 0.95, 'green': 0.8,  'blue': 0.8},
#                 'environment':  {'red': 0.8,  'green': 0.95, 'blue': 0.8},
#                 'development':  {'red': 0.8,  'green': 0.9,  'blue': 1.0},
#                 'local_events': {'red': 1.0,  'green': 0.9,  'blue': 0.8},
#                 'health':       {'red': 0.85, 'green': 0.95, 'blue': 0.95},
#                 'achievement':  {'red': 1.0,  'green': 0.95, 'blue': 0.7},
#                 'politics':     {'red': 0.9,  'green': 0.85, 'blue': 1.0},
#                 'general':      {'red': 1.0,  'green': 1.0,  'blue': 0.9},
#             }
#             worksheet.format(f'B{next_row}', {
#                 'textFormat': {
#                     'bold': True,
#                     'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
#                     'fontSize': 10
#                 },
#                 'backgroundColor': category_colors.get(category, category_colors['general']),
#                 'horizontalAlignment': 'CENTER'
#             })

#             print(f"✅ Saved [{category.upper()}] {title[:50]}...")
#             return True

#         except gspread.exceptions.APIError as e:
#             error_str = str(e)
#             if any(code in error_str for code in ['503', '500', '429', '502']):
#                 if attempt < max_retries:
#                     print(f"   ⚠️ Sheets 503 on save (attempt {attempt}/{max_retries}) — retrying in 8s...")
#                     time.sleep(8)
#                     continue
#                 else:
#                     print(f"   ❌ Save failed after {max_retries} retries: {e}")
#                     return False
#             print(f"❌ Save error: {e}")
#             return False
#         except Exception as e:
#             print(f"❌ Save error: {e}")
#             return False


# # ============================================================
# # Helpers
# # ============================================================
# def get_content_hash(title: str, content: str) -> str:
#     return hashlib.md5(
#         f"{title.lower()}{content[:200].lower()}".encode()
#     ).hexdigest()


# def sort_by_count(item):
#     return -item[1]


# def sort_by_priority(item):
#     return {'high': 1, 'medium': 2, 'low': 3}.get(item.get('importance', 'medium'), 2)


# def safe_truncate(text: str, max_chars: int) -> str:
#     if len(text) <= max_chars:
#         return text
#     truncated = text[:max_chars]
#     for punct in ['।', '.', '!', '?', '\n']:
#         last_pos = truncated.rfind(punct)
#         if last_pos > max_chars * 0.7:
#             return truncated[:last_pos + 1]
#     last_space = truncated.rfind(' ')
#     if last_space > max_chars * 0.7:
#         return truncated[:last_space]
#     return truncated


# def get_cta(category: str) -> str:
#     return CATEGORY_CTA.get(category.lower().strip(), DEFAULT_CTA)


# def is_thane_related(title: str, url: str) -> bool:
#     combined = (title + " " + url).lower()
#     return any(kw.lower() in combined for kw in THANE_KEYWORDS)


# def extract_response_content(response) -> str:
#     raw_choice = response.choices[0]
#     if hasattr(raw_choice, 'message'):
#         msg = raw_choice.message
#         if hasattr(msg, 'content') and isinstance(msg.content, str):
#             return msg.content
#         elif hasattr(msg, 'content') and isinstance(msg.content, list):
#             return ' '.join(
#                 block.get('text', '') if isinstance(block, dict) else str(block)
#                 for block in msg.content
#             )
#         elif isinstance(msg, list):
#             return ' '.join(
#                 block.get('text', '') if isinstance(block, dict) else str(block)
#                 for block in msg
#             )
#         else:
#             return str(msg)
#     elif isinstance(raw_choice, dict):
#         msg = raw_choice.get('message', {})
#         return msg.get('content', '') if isinstance(msg, dict) else str(msg)
#     else:
#         return str(raw_choice)


# # ============================================================
# # Script Completion Check — uses partial "सलाम ठाणे" match
# # ============================================================
# def is_script_complete(script: str, category: str = "general") -> bool:
#     return "फॉलो करा सलाम ठाणे" in script


# def get_last_line(script: str) -> str:
#     lines = [l.strip() for l in script.strip().split('\n') if l.strip()]
#     return lines[-1] if lines else ""


# async def complete_script_if_needed(script: str, news_article: Dict) -> str:
#     global total_input_tokens, total_output_tokens, total_cost

#     category = news_article.get('category', 'general')
#     cta      = get_cta(category)

#     if is_script_complete(script, category):
#         return script

#     last_line = get_last_line(script)
#     print(f"   🔧 Script incomplete — last: '{last_line[:60]}' — completing...")

#     try:
#         response = perplexity_client.chat.completions.create(
#             model=SCRIPT_MODEL,
#             messages=[
#                 {
#                     "role": "system",
#                     "content": f'फक्त मराठी lines लिहा. शेवटची line नक्की हीच: "{cta}"'
#                 },
#                 {
#                     "role": "user",
#                     "content": f"""खालील अर्धवट मराठी script पूर्ण करा.

# अर्धवट script:
# {script}

# नियम:
# - वरील script च्या पुढे फक्त उर्वरित lines लिहा
# - एकूण 9-14 lines होतील असे नवीन lines जोडा
# - शेवटची line नक्की हीच: "{cta}"
# - फक्त नवीन lines लिहा, जुन्या lines परत लिहू नका
# - फक्त मराठीत लिहा"""
#                 }
#             ],
#             temperature=0.7,
#             max_tokens=400
#         )

#         if hasattr(response, 'usage'):
#             i_t = response.usage.prompt_tokens
#             o_t = response.usage.completion_tokens
#             total_input_tokens  += i_t
#             total_output_tokens += o_t
#             total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

#         completion = extract_response_content(response).strip()
#         completion = re.sub(r'<think>.*?</think>', '', completion, flags=re.DOTALL).strip()
#         completion = completion.replace('```', '').strip()

#         if any(kw.lower() in completion.lower() for kw in REFUSAL_KEYWORDS):
#             print(f"   ⚠️ Completion refused — appending CTA directly")
#             return script.strip() + f"\n\n{cta}"

#         completed = script.strip() + "\n\n" + completion.strip()
#         if not is_script_complete(completed, category):
#             completed = completed.strip() + f"\n\n{cta}"

#         print(f"   ✅ Script completed")
#         return completed

#     except Exception as e:
#         error_str = str(e).lower()
#         if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
#             raise CreditExhaustedException(str(e))
#         print(f"   ⚠️ Completion error: {e} — appending CTA directly")
#         return script.strip() + f"\n\n{cta}"


# # ============================================================
# # Marathi Validator
# # ============================================================
# def is_valid_marathi_script(script: str) -> bool:
#     if len(script) < 80:
#         return False
#     if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
#         return False
#     devanagari = len(re.findall(r'[\u0900-\u097F]', script))
#     total      = len(script.replace(' ', '').replace('\n', ''))
#     return (devanagari / max(total, 1)) > 0.35


# # ============================================================
# # API Credit Check
# # ============================================================
# async def check_api_credits():
#     try:
#         perplexity_client.chat.completions.create(
#             model=ANALYSIS_MODEL,
#             messages=[{"role": "user", "content": "ok"}],
#             max_tokens=1
#         )
#         print("✅ API credits OK")
#         return True
#     except Exception as e:
#         error_str = str(e).lower()
#         if any(code in error_str for code in [
#             '402', '429', '401', 'insufficient', 'credit',
#             'quota', 'balance', 'payment', 'billing', 'rate limit', 'exceeded'
#         ]):
#             print("=" * 60)
#             print("❌ PERPLEXITY API CREDITS EXHAUSTED!")
#             print(f"   Error: {str(e)}")
#             print("=" * 60)
#             print("👉 Top up: https://www.perplexity.ai/settings/api")
#             return False
#         print(f"❌ Unknown API error: {e}")
#         return False


# # ============================================================
# # Fetch Article with Retry
# # ============================================================
# async def fetch_article_with_retry(crawler, url: str, retries: int = 3) -> str:
#     for attempt in range(1, retries + 1):
#         try:
#             result = await crawler.arun(
#                 url,
#                 config=CrawlerRunConfig(
#                     cache_mode=CacheMode.BYPASS,
#                     word_count_threshold=10,
#                     page_timeout=25000
#                 )
#             )
#             if result.success and len(result.markdown) > 50:
#                 return result.markdown
#             await asyncio.sleep(2)
#         except Exception:
#             await asyncio.sleep(2)
#     return ""


# # ============================================================
# # Scraping — Thane-Only
# # ============================================================
# async def scrape_thane_news_sources():
#     all_news = []

#     async with AsyncWebCrawler(verbose=False) as crawler:
#         for site in NEWS_SITES:
#             print(f"\n{'='*60}")
#             print(f"🔍 {site['name']} | Target: {site['target']}")
#             print(f"{'='*60}")

#             site_articles = []

#             try:
#                 result = await crawler.arun(
#                     site['url'],
#                     config=CrawlerRunConfig(
#                         cache_mode=CacheMode.BYPASS,
#                         wait_for="body",
#                         word_count_threshold=10,
#                         page_timeout=30000,
#                         js_code="await new Promise(r => setTimeout(r, 3000));"
#                     )
#                 )

#                 if not result.success:
#                     print(f"❌ Failed: {site['name']}")
#                     continue

#                 soup = BeautifulSoup(result.html, 'html.parser')
#                 raw_articles = []

#                 for link_tag in soup.find_all('a', href=True):
#                     href  = link_tag.get('href', '')
#                     title = link_tag.get_text(strip=True)

#                     if any(kw.lower() in title.lower() for kw in SKIP_TITLE_KEYWORDS):
#                         continue

#                     if any(x in href.lower() for x in SKIP_URL_PATTERNS):
#                         continue

#                     if not (15 < len(title) < 300):
#                         continue

#                     if site['link_pattern'] not in href:
#                         continue

#                     # ✅ Thane filter: skip non-Thane articles on general sources
#                     if site.get('thane_strict', True):
#                         if not is_thane_related(title, href):
#                             continue

#                     if href.startswith('/'):
#                         base = site['url'].split('/')[0] + '//' + site['url'].split('/')[2]
#                         href = base + href

#                     if href.startswith('http'):
#                         raw_articles.append({'title': title, 'link': href})

#                 seen = set()
#                 unique_links = []
#                 for a in raw_articles:
#                     if a['link'] not in seen:
#                         unique_links.append(a)
#                         seen.add(a['link'])

#                 print(f"📋 Found {len(unique_links)} unique Thane links")

#                 for article in unique_links:
#                     if len(site_articles) >= site['target']:
#                         break

#                     print(f"   🔗 [{len(site_articles)+1}/{site['target']}] "
#                           f"{article['title'][:50]}...")

#                     markdown = await fetch_article_with_retry(crawler, article['link'])
#                     content  = markdown if markdown else article['title']

#                     if any(kw.lower() in content.lower() for kw in SKIP_CONTENT_KEYWORDS):
#                         print(f"   ⏭️  Skipped (utility/spiritual content)")
#                         continue

#                     content_hash = get_content_hash(article['title'], content)

#                     if content_hash not in processed_hashes:
#                         site_articles.append({
#                             'title':            article['title'],
#                             'link':             article['link'],
#                             'content':          safe_truncate(content, 3500),
#                             'hash':             content_hash,
#                             'has_full_content': bool(markdown)
#                         })
#                         processed_hashes.add(content_hash)
#                         tag = "✅" if markdown else "⚠️ fallback"
#                         print(f"   {tag} [{len(site_articles)}/{site['target']}] "
#                               f"{article['title'][:50]}...")
#                     else:
#                         print(f"   🔄 Duplicate skipped")

#                     await asyncio.sleep(1)

#                 print(f"\n📦 {site['name']}: {len(site_articles)}/{site['target']} collected")

#                 if site_articles:
#                     filtered = await smart_analyze_with_category(
#                         site_articles, site['name']
#                     )
#                     all_news.extend(filtered)
#                     print(f"🧠 {site['name']}: Analyzed {len(filtered)} articles")

#             except Exception as e:
#                 print(f"❌ Error {site['name']}: {e}")

#             await asyncio.sleep(3)

#     return all_news


# # ============================================================
# # AI Categorization — Thane-Specific Categories
# # ============================================================
# async def smart_analyze_with_category(articles: List[Dict], source_name: str):
#     global total_input_tokens, total_output_tokens, total_cost

#     all_filtered = []

#     for batch_start in range(0, len(articles), 5):
#         raw_batch = articles[batch_start:batch_start + 5]

#         batch = [
#             article for article in raw_batch
#             if not any(kw.lower() in article.get('content', '').lower()
#                        for kw in SKIP_CONTENT_KEYWORDS)
#         ]

#         if not batch:
#             print(f"   ⏭️  Skipped batch — all utility/spiritual articles")
#             continue

#         index_to_link  = {i: article['link']  for i, article in enumerate(batch)}
#         index_to_title = {i: article['title'] for i, article in enumerate(batch)}

#         articles_text = ""
#         for idx, article in enumerate(batch):
#             articles_text += f"INDEX_{idx}: {article['title']}\n{safe_truncate(article['content'], 500)}\n---\n"

#         prompt = f"""ठाणे जिल्ह्यातील मराठी बातम्या विश्लेषक: खालील बातम्यांना category आणि Marathi summary द्या.

# ⚠️ नियम:
# 1. detailed_summary आणि key_points फक्त मराठीत लिहा
# 2. JSON मध्ये "index" field EXACTLY जसा दिला (0,1,2,3,4) तसाच परत द्या
# 3. title आणि link field नको — फक्त index वापरा
# 4. फक्त ठाणे जिल्ह्याशी थेट संबंधित बातम्यांना "high" importance द्या

# Categories (फक्त यापैकी एक वापरा):
# - crime       → गुन्हे, अपघात, मृत्यू, अटक
# - environment → झाडे, प्रदूषण, वनवा, उद्यान
# - development → मेट्रो, रस्ते, पूल, TMC प्रकल्प
# - local_events → उत्सव, रॅली, स्पर्धा, सोहळे
# - health      → आरोग्य सेवा, रुग्णालय, योजना
# - achievement → विक्रम, पुरस्कार, यश
# - politics    → राजकारण, निवडणूक, आंदोलन
# - general     → इतर सर्व

# JSON array format:
# [{{"index": 0, "category": "cat", "detailed_summary": "मराठी सारांश १००-१५० शब्द", "importance": "high/medium/low", "key_points": ["मुद्दा १", "मुद्दा २", "मुद्दा ३"]}}]

# बातम्या:
# {articles_text}

# फक्त JSON array. Index 0 ते {len(batch)-1} पर्यंत."""

#         try:
#             response = perplexity_client.chat.completions.create(
#                 model=ANALYSIS_MODEL,
#                 messages=[
#                     {
#                         "role": "system",
#                         "content": "Return ONLY valid JSON array. Use index field (0,1,2...). No title or link fields."
#                     },
#                     {"role": "user", "content": prompt}
#                 ],
#                 temperature=0.2,
#                 max_tokens=3000
#             )

#             if hasattr(response, 'usage'):
#                 i_t = response.usage.prompt_tokens
#                 o_t = response.usage.completion_tokens
#                 total_input_tokens  += i_t
#                 total_output_tokens += o_t
#                 c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
#                 total_cost += c
#                 print(f"   📊 {i_t}in + {o_t}out = ${c:.4f}")

#             content = extract_response_content(response)

#             if not content.strip():
#                 print(f"   ⚠️ Empty response!")
#                 raise ValueError("Empty content from API")

#             content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
#             match = re.search(r'\[.*\]', content, re.DOTALL)

#             if match:
#                 parsed = json.loads(match.group())

#                 for art in parsed:
#                     idx = art.get('index')
#                     if idx is not None and idx in index_to_link:
#                         art['link']  = index_to_link[idx]
#                         art['title'] = index_to_title[idx]
#                     else:
#                         pos = len(all_filtered) % len(batch)
#                         art['link']  = index_to_link.get(pos, '')
#                         art['title'] = index_to_title.get(pos, art.get('title', ''))

#                     if art.get('category') not in VALID_CATEGORIES:
#                         art['category'] = 'general'

#                 all_filtered.extend(parsed)
#                 print(f"   ✅ Categorized {len(parsed)} | Links: INDEX-matched ✅")

#             else:
#                 print(f"   ⚠️ No JSON found — raw: {content[:200]}")
#                 for i, article in enumerate(batch):
#                     all_filtered.append({
#                         'index':            i,
#                         'title':            article['title'],
#                         'category':         'general',
#                         'detailed_summary': safe_truncate(article['content'], 600),
#                         'importance':       'medium',
#                         'link':             article['link'],
#                         'key_points':       [article['title']]
#                     })

#         except json.JSONDecodeError as e:
#             print(f"   ❌ JSON parse error: {e}")
#             for i, article in enumerate(batch):
#                 all_filtered.append({
#                     'index':            i,
#                     'title':            article['title'],
#                     'category':         'general',
#                     'detailed_summary': safe_truncate(article['content'], 600),
#                     'importance':       'medium',
#                     'link':             article['link'],
#                     'key_points':       [article['title']]
#                 })

#         except Exception as e:
#             error_str = str(e).lower()
#             if any(code in error_str for code in [
#                 '402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing'
#             ]):
#                 print(f"\n💳 CREDITS EXHAUSTED during analysis!")
#                 raise CreditExhaustedException(str(e))
#             print(f"   ❌ AI error: {e}")
#             for i, article in enumerate(batch):
#                 all_filtered.append({
#                     'index':            i,
#                     'title':            article['title'],
#                     'category':         'general',
#                     'detailed_summary': safe_truncate(article['content'], 600),
#                     'importance':       'medium',
#                     'link':             article['link'],
#                     'key_points':       [article['title'], safe_truncate(article['content'], 100)]
#                 })

#         await asyncio.sleep(1.5)

#     for art in all_filtered:
#         art['source']     = source_name
#         art['scraped_at'] = datetime.now(IST).isoformat()

#     return all_filtered


# # ============================================================
# # Script Generation — 100% Salaam Thane Style
# # ============================================================
# async def create_reel_script_single(news_article: Dict):
#     global total_input_tokens, total_output_tokens, total_cost

#     category = news_article.get('category', 'general')
#     cta      = get_cta(category)

#     # ✅ Category-specific hook templates — extracted from real Salaam Thane reels
#     hook_templates = {
#         "crime":        "ज्या वयात [innocence]... त्याच वयात [tragedy]... (emotional contrast)",
#         "environment":  "ठाणेकरांनो सावधान — [problem statement] (direct alarm hook)",
#         "development":  "[benefit] येणार, पण त्याची डार्क साईड माहिती आहे? (twist hook)",
#         "local_events": "[visual scene: outfit/setting/people]. ठाण्याच्या [audience group] तयार व्हा! (scene-setting hook)",
#         "health":       "आता [service/facility] होणार स्मार्ट — [location] मध्ये [benefit]. (benefit-first hook)",
#         "achievement":  "[visual scene]. जेव्हा [action] होतो, तेव्हा काय होतं माहिती आहे? [reveal]. (reveal hook)",
#         "politics":     "ठाण्यातील राजकारण आता [adjective] झालंय... (drama hook)",
#         "general":      "ठाणे जिल्ह्यात एक [adjective] घटना घडली आहे... (direct hook)",
#     }

#     hook_guide = hook_templates.get(category, hook_templates["general"])

#     system_prompt = f"""तुम्ही "सलाम ठाणे" या Instagram channel चे मराठी Reel script writer आहात.
# फक्त मराठी भाषेत लिहा. तुम्ही AI आहात हे कधीही सांगू नका. फक्त script लिहा.

# 🎯 Channel Focus: फक्त ठाणे जिल्ह्यातील hyperlocal बातम्या
# 📍 Local areas: ठाणे, डोंबिवली, कल्याण, भिवंडी, उल्हासनगर, अंबरनाथ, मीरा रोड, बदलापूर, पारसिक, मुंब्रा, कोपरी

# Script Structure (9-14 lines total):
# - Line 1-2: HOOK — {hook_guide}
# - Line 3-9: सर्व facts — नावे, ठिकाण, तारीख, अचूक संख्या सह
# - Line 10-12: प्रश्न / विश्लेषण / twist
# - Line 13-14: audience question + CTA

# ✅ MUST use at least 3 of these Salaam Thane signature phrases:
# → "तब्बल [number]" — for shocking stats (e.g. तब्बल 9111 विद्यार्थिनी)
# → "विशेष म्हणजे" — to flag a key highlight
# → "सर्वात महत्त्वाचं म्हणजे" — for the main point
# → "पण इथेच खरा प्रश्न उभा राहतो" — for controversy/twist
# → "अक्षरशः" — for dramatic emphasis
# → "सर्वात चीड यायची गोष्ट म्हणजे" — for outrage (crime/injustice)
# → "आपल्या [ठाण्याच्या/डोंबिवलीच्या/कल्याणच्या]..." — local ownership
# → "कौतुक आहे [person/institution] चं" — for positive stories

# कठोर नियम:
# - 9-14 lines total, प्रत्येक line 1-2 sentences
# - संपूर्ण output फक्त मराठीत (proper nouns सोडून)
# - कोणतेही heading, explanation, markdown नाही
# - "माहिती नाही", "खेद आहे", "मी Perplexity" असे कधीही लिहू नका
# - शेवटची line नक्की हीच: "{cta}"
# - script अर्धवट सोडू नका — शेवटपर्यंत लिहा"""

#     summary    = safe_truncate(
#         news_article.get('detailed_summary', news_article.get('title', '')), 600
#     )
#     key_points = ', '.join(news_article.get('key_points', [news_article.get('title', '')]))

#     user_prompt_v1 = f"""Category: {category.upper()}
# शीर्षक: {news_article['title']}
# सारांश: {summary}
# मुद्दे: {key_points}

# वरील ठाणे जिल्ह्यातील बातमीचे facts वापरून 9-14 मराठी lines तयार करा.
# "तब्बल", "विशेष म्हणजे", "सर्वात महत्त्वाचं म्हणजे" यापैकी किमान 3 phrases वापरा.
# शेवटची line नक्की: "{cta}"
# जरी माहिती कमी असली तरी उपलब्ध तथ्यांवर आधारित पूर्ण script लिहा."""

#     user_prompt_v2 = f"""खालील ठाणे जिल्ह्यातील बातमीवर 10 मराठी वाक्ये लिहा.
# बातमी: {news_article['title']}. {safe_truncate(summary, 200)}
# - प्रत्येक वाक्य नवीन line वर
# - फक्त मराठीत
# - "तब्बल" किंवा "विशेष म्हणजे" नक्की वापरा
# - शेवटची line नक्की: "{cta}" """

#     prompts = [user_prompt_v1, user_prompt_v2]

#     for attempt in range(1, 3):
#         try:
#             response = perplexity_client.chat.completions.create(
#                 model=SCRIPT_MODEL,
#                 messages=[
#                     {"role": "system", "content": system_prompt},
#                     {"role": "user",   "content": prompts[attempt - 1]}
#                 ],
#                 temperature=0.8,
#                 max_tokens=1500
#             )

#             if hasattr(response, 'usage'):
#                 i_t = response.usage.prompt_tokens
#                 o_t = response.usage.completion_tokens
#                 total_input_tokens  += i_t
#                 total_output_tokens += o_t
#                 total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

#             script = extract_response_content(response).strip()
#             script = re.sub(r'<think>.*?</think>', '', script, flags=re.DOTALL).strip()
#             script = script.replace('```', '').strip()

#             if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
#                 print(f"   ⚠️ Attempt {attempt}: Refusal — retrying...")
#                 continue

#             if is_valid_marathi_script(script):
#                 script = await complete_script_if_needed(script, news_article)
#                 return script

#             print(f"   ⚠️ Attempt {attempt}: Not valid Marathi — retrying...")

#         except CreditExhaustedException:
#             raise
#         except Exception as e:
#             error_str = str(e).lower()
#             if any(code in error_str for code in [
#                 '402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing'
#             ]):
#                 raise CreditExhaustedException(str(e))
#             print(f"   ⚠️ Attempt {attempt} error: {e}")
#             await asyncio.sleep(2)

#     # ✅ Fallback — Salaam Thane styled
#     title_fb   = news_article.get('title', 'एक महत्त्वाची बातमी')[:80]
#     summary_fb = safe_truncate(
#         news_article.get('detailed_summary', news_article.get('title', '')), 200
#     )
#     cta_fb = get_cta(category)

#     return f"""{title_fb}

# {summary_fb}

# ही घटना ठाणे जिल्ह्यात मोठी चर्चा निर्माण करत आहे.

# विशेष म्हणजे या प्रकरणात अनेक महत्त्वाचे प्रश्न उपस्थित होत आहेत.

# सर्वात महत्त्वाचं म्हणजे सर्वसामान्य ठाणेकरांवर याचा थेट परिणाम होणार आहे.

# पण इथेच खरा प्रश्न उभा राहतो — प्रशासन नक्की काय पाऊल उचलणार?

# {cta_fb}"""


# # ============================================================
# # Main Pipeline
# # ============================================================
# async def main():
#     global total_input_tokens, total_output_tokens, total_cost

#     print("=" * 70)
#     print("🟢 SALAAM THANE — HYPERLOCAL REEL SCRIPT GENERATOR v1.0")
#     print(f"🔍 Analysis : {ANALYSIS_MODEL}")
#     print(f"✍️  Scripts  : {SCRIPT_MODEL}")
#     print(f"🎯 Target   : {TARGET_SCRIPTS} scripts")
#     print(f"📍 Focus    : ठाणे जिल्हा — Hyperlocal Only")
#     print(f"🕐 Timezone : IST (Asia/Kolkata)")
#     print("=" * 70)

#     credits_ok = await check_api_credits()
#     if not credits_ok:
#         print("\n🛑 Stopping. Top up credits first.")
#         print("👉 https://www.perplexity.ai/settings/api")
#         return

#     start_time = datetime.now(IST)

#     print("\n" + "=" * 70)
#     print("STEP 1: SCRAPING 5 THANE NEWS SOURCES")
#     print("=" * 70 + "\n")

#     try:
#         all_articles = await scrape_thane_news_sources()
#     except CreditExhaustedException:
#         print("\n🛑 Credits exhausted during scraping. Stopping.")
#         return

#     unique_articles = []
#     seen_hashes = set()
#     for article in all_articles:
#         h = article.get('hash', get_content_hash(
#             article['title'], article.get('detailed_summary', '')
#         ))
#         if h not in seen_hashes:
#             unique_articles.append(article)
#             seen_hashes.add(h)

#     print(f"\n✅ Total unique Thane articles: {len(unique_articles)}")

#     if len(unique_articles) < TARGET_SCRIPTS:
#         print(f"⚠️  Only {len(unique_articles)} articles — generating {len(unique_articles)} scripts")

#     category_counts = {}
#     for article in unique_articles:
#         cat = article.get('category', 'general')
#         category_counts[cat] = category_counts.get(cat, 0) + 1

#     print("\n📊 Category Breakdown:")
#     for cat, count in sorted(category_counts.items(), key=sort_by_count):
#         print(f"   {cat.upper():<15} {'█' * count} ({count})")

#     unique_articles.sort(key=sort_by_priority)
#     selected_articles = unique_articles[:TARGET_SCRIPTS]

#     print(f"\n🎯 Selected : {len(selected_articles)}/{TARGET_SCRIPTS}")
#     print(f"⏱️  Scraping : {(datetime.now(IST)-start_time).total_seconds():.0f}s\n")

#     print("=" * 70)
#     print("STEP 2: GENERATING SCRIPTS → GOOGLE SHEETS")
#     print("=" * 70 + "\n")

#     worksheet        = setup_google_sheets()
#     successful_saves = 0
#     failed_saves     = 0

#     if worksheet and selected_articles:
#         for idx, article in enumerate(selected_articles, 1):
#             cat = article.get('category', 'general')
#             print(f"\n[{idx:02d}/{len(selected_articles)}] "
#                   f"{article.get('source','')[:12]} | "
#                   f"{cat.upper():<13} | "
#                   f"{article['title'][:40]}...")

#             try:
#                 script = await create_reel_script_single(article)
#             except CreditExhaustedException:
#                 print(f"\n🛑 Credits exhausted at script {idx}/{len(selected_articles)}")
#                 print(f"   ✅ Saved so far: {successful_saves} scripts")
#                 print(f"👉 Top up: https://www.perplexity.ai/settings/api")
#                 break

#             dev_chars   = len(re.findall(r'[\u0900-\u097F]', script))
#             total_ch    = len(script.replace(' ', '').replace('\n', ''))
#             marathi_pct = (dev_chars / max(total_ch, 1)) * 100
#             lang_tag    = "🇮🇳" if marathi_pct > 35 else "⚠️"
#             cta_tag     = "✅" if is_script_complete(script, cat) else "⚠️ NO CTA"
#             print(f"   📝 {lang_tag} ({marathi_pct:.0f}%) | CTA:{cta_tag} | "
#                   f"🔗 {article.get('link','')[:55]}...")

#             success = save_to_google_sheets(
#                 worksheet,
#                 cat,
#                 article['title'],
#                 script,
#                 article.get('link', '')
#             )
#             if success:
#                 successful_saves += 1
#             else:
#                 failed_saves += 1

#             await asyncio.sleep(1)

#     total_duration = (datetime.now(IST) - start_time).total_seconds()
#     total_tokens   = total_input_tokens + total_output_tokens

#     print("\n" + "=" * 70)
#     print("📈 FINAL SUMMARY — SALAAM THANE")
#     print("=" * 70)
#     print(f"   🔍 Analysis model     : {ANALYSIS_MODEL}")
#     print(f"   ✍️  Script model       : {SCRIPT_MODEL}")
#     print(f"   📰 Articles scraped   : {len(unique_articles)}")
#     print(f"   ✅ Scripts saved      : {successful_saves}")
#     print(f"   ❌ Failed             : {failed_saves}")
#     print(f"   ⏱️  Total time         : {total_duration:.0f}s ({total_duration/60:.1f} mins)")
#     print(f"   📥 Input tokens       : {total_input_tokens:,}")
#     print(f"   📤 Output tokens      : {total_output_tokens:,}")
#     print(f"   🔢 Total tokens       : {total_tokens:,}")
#     print(f"   💰 Total cost         : ${total_cost:.4f} (~₹{total_cost*84:.2f})")
#     print(f"   💵 Cost per script    : ${total_cost/max(successful_saves,1):.4f}")
#     print(f"   🕐 Finished at (IST)  : {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
#     if worksheet:
#         print(f"   📊 Sheet URL          : https://docs.google.com/spreadsheets/d/"
#               f"{worksheet.spreadsheet.id}")
#     print("=" * 70 + "\n")


# if __name__ == "__main__":
#     asyncio.run(main())



import asyncio
import json
from datetime import datetime
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup
from openai import OpenAI
import re
import gspread
from google.oauth2.service_account import Credentials
import os
from typing import List, Dict
import hashlib
import pytz
import time


# ============================================================
# Custom Exceptions
# ============================================================
class CreditExhaustedException(Exception):
    pass


# ============================================================
# Perplexity Client
# ============================================================
perplexity_client = OpenAI(
    api_key=os.environ.get("PERPLEXITY_API_KEY"),
    base_url="https://api.perplexity.ai"
)

ANALYSIS_MODEL       = "sonar-pro"
SCRIPT_MODEL         = "sonar-reasoning-pro"

ANALYSIS_INPUT_COST  = 1.0 / 1_000_000
ANALYSIS_OUTPUT_COST = 1.0 / 1_000_000
SCRIPT_INPUT_COST    = 2.0 / 1_000_000
SCRIPT_OUTPUT_COST   = 8.0 / 1_000_000

IST = pytz.timezone('Asia/Kolkata')


# ============================================================
# Config
# ============================================================
GOOGLE_SHEETS_CREDENTIALS_FILE = "credentials.json"
GOOGLE_SHEET_NAME               = "Salam Thane"
GOOGLE_WORKSHEET_NAME           = "Sheet1"
TARGET_SCRIPTS                  = 10

VALID_CATEGORIES = [
    "crime", "environment", "development", "local_events",
    "health", "achievement", "politics", "general"
]

CATEGORY_CTA = {
    "crime":        "या प्रकरणावर तुम्हाला काय वाटतं? कमेंट्स मध्ये नक्की सांगा आणि अशाच महत्त्वाच्या अपडेटसाठी फॉलो करा सलाम ठाणे.",
    "environment":  "तुम्हाला काय वाटतं? कमेंट्स मध्ये नक्की सांगा आणि अशाच महत्त्वाच्या अपडेटसाठी फॉलो करा सलाम ठाणे.",
    "development":  "तुमचं या विकासकामाबद्दल काय मत आहे? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे.",
    "local_events": "तुम्ही यात सहभागी होणार का? कमेंट्स मध्ये सांगा आणि अशाच झन्नाट अपडेट साठी फॉलो करा सलाम ठाणे.",
    "health":       "तुम्हाला हा उपक्रम कसा वाटला? कमेंट्स मध्ये सांगा आणि अशाच भन्नाट अपडेटसाठी फॉलो करा सलाम ठाणे.",
    "achievement":  "या ऐतिहासिक क्षणाबद्दल तुमचं काय मत आहे? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे.",
    "politics":     "या राजकारणाबद्दल तुम्हाला काय वाटतं? कमेंट्स मध्ये नक्की सांगा आणि अशाच महत्त्वाच्या अपडेटसाठी फॉलो करा सलाम ठाणे.",
    "general":      "तुम्हाला काय वाटतं? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे.",
}
DEFAULT_CTA = "तुम्हाला काय वाटतं? कमेंट्स मध्ये सांगा आणि अशाच अपडेटसाठी फॉलो करा सलाम ठाणे."

REFUSAL_KEYWORDS = [
    "I appreciate", "I should clarify", "I'm Perplexity",
    "search assistant", "I'm not able", "I cannot create",
    "Would you like", "clarify my role", "I'm an AI",
    "as an AI", "I don't create",
    "मुझे खेद है", "मैं इस अनुरोध", "खोज परिणामों में",
    "प्रदान किए गए", "कृपया स्पष्ट करें", "मैं सही तरीके",
    "विशिष्ट तथ्य नहीं", "आवश्यक माहिती",
    "मला खेद आहे", "मला क्षमस्व", "उत्तर देण्यासाठी आवश्यक",
    "शोध परिणामांमध्ये", "कृपया एक पूर्ण बातमी",
    "अधिक संबंधित शोध", "विशिष्ट घटना", "तपशील पुनः तपास",
    "मी Perplexity", "मी perplexity", "माझी भूमिका",
    "मूळ कार्याच्या विरुद्ध", "script लिहिण्याची विनंती",
    "सूचना देणे", "संशोधित उत्तरे", "मी एक AI",
    "script writer नाही", "मी तयार करू शकत नाही",
    "शोध निकालांमध्ये", "मेल होत नाही", "script तयार करू शकतो पण",
    "विस्तृत search results", "स्पष्ट करा"
]

SKIP_TITLE_KEYWORDS = [
    'राशीभविष्य', 'राशिभविष्य', 'ज्योतिष', 'पूजा', 'अध्यात्म',
    'horoscope', 'rashifal', 'astrology', 'dharm', 'puja',
    'utility', 'यूटिलिटी', 'आध्यात्मिक', 'spirituality',
    'धार्मिक परंपरा', 'मंदिर', 'व्रत', 'उपवास', 'rashibhavishya',
    'धार्मिक', 'ज्योतिष', 'recipe', 'cooking', 'फॅशन',
    'bollywood', 'cricket', 'ipl', 'sensex', 'nifty',
    'share market', 'stock', 'weather', 'हवामान',
]

SKIP_CONTENT_KEYWORDS = [
    'tv9 मराठी एक 24/7 मराठी भाषिक वृत्तवाहिनी',
    'अध्यात्म बातम्यांचा विशेष विभाग आहे जो',
    'राशीभविष्य, मंदिरातील पूजा, धार्मिक परंपरा',
    'आध्यात्मिक जीवनाची संपूर्ण माहिती',
    'यूटिलिटी बातम्या म्हणजे काय',
    'utility news definition'
]

THANE_KEYWORDS = [
    'thane', 'ठाण', 'dombivli', 'डोंबिवली', 'kalyan', 'कल्याण',
    'bhiwandi', 'भिवंडी', 'ulhasnagar', 'उल्हासनगर',
    'mira-road', 'miraroad', 'मीरा रोड', 'ambernath', 'अंबरनाथ',
    'badlapur', 'बदलापूर', 'titwala', 'टिटवाळा',
    'shahad', 'vithalwadi', 'kdmc', 'tmc', 'kopri', 'mumbra',
    'wagle', 'majiwada', 'kasarwadavali', 'palghar',
    'murbad', 'shahapur', 'vasai', 'virar', 'nalasopara',
]

SKIP_URL_PATTERNS = [
    'javascript:', 'mailto:', '#',
    '/category/', '/tag/', '/author/',
    'facebook.com', 'twitter.com', 'instagram.com',
    'youtube.com', 'whatsapp.com', '/myaccount/',
    '/install_app', '/advertisement', '/epaper',
    'web-stories', 'photo-gallery', '/videos/',
    '/games/', '/jokes/', '/terms-and-conditions',
    '/topic/', '/widget/', '/livetv',
    'articlelist', '/live',
    '/utility/', '/utilities/',
    '/adhyatma/', '/astrology/', '/rashifal/',
    '/horoscope/', '/jyotish/', '/puja/',
    '/dharm/', '/dharma/', '/spirituality/',
    '/rashibhavishya/', '/religion/',
    '/north', '/south', '/west', '/east',
    '/nashik', '/aurangabad', '/nagpur', '/konkan',
    '/vidarbha', '/marathwada',
    '/pune', '/solapur', '/kolhapur', '/satara',
    '/entertainment', '/bollywood', '/sports', '/cricket',
    '/business', '/technology', '/health/national',
]


# ============================================================
# Token Tracking
# ============================================================
total_input_tokens  = 0
total_output_tokens = 0
total_cost          = 0.0
processed_hashes    = set()


# ============================================================
# News Sites — 12 Sources (Original 5 + 7 New)
# Targets tuned so sum ≥ TARGET_SCRIPTS (10)
# ============================================================
NEWS_SITES = [
    # ── Original 5 ──────────────────────────────────────────
    {
        "name": "Maharashtra Times Thane",
        "url": "https://maharashtratimes.com/maharashtra/thane-news",
        "link_pattern": "maharashtratimes.com",
        "thane_strict": False,
        "target": 3,
        "fetch_limit": 30
    },
    {
        "name": "Lokmat Thane",
        "url": "https://www.lokmat.com/thane/",
        "link_pattern": "lokmat.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "ABP Majha Thane",
        "url": "https://marathi.abplive.com/news/thane",
        "link_pattern": "abplive.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "TV9 Marathi Thane",
        "url": "https://www.tv9marathi.com/city/thane",
        "link_pattern": "tv9marathi.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "Navshakti Thane",
        "url": "https://www.navshakti.co.in/thane/",
        "link_pattern": "navshakti.co.in",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 20
    },
    # ── 7 New Sources ────────────────────────────────────────
    {
        "name": "Navbharat Live Thane",
        "url": "https://navbharatlive.com/maharashtra/thane",
        "link_pattern": "navbharatlive.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "NDTV Thane",
        "url": "https://www.ndtv.com/topic/thane",
        "link_pattern": "ndtv.com",
        "thane_strict": True,   # ← strict: only articles mentioning Thane keywords
        "target": 2,
        "fetch_limit": 30
    },
    {
        "name": "Indian Express Thane",
        "url": "https://indianexpress.com/about/thane/",
        "link_pattern": "indianexpress.com",
        "thane_strict": True,
        "target": 2,
        "fetch_limit": 30
    },
    {
        "name": "Lokmat Thane (direct)",
        "url": "https://www.lokmat.com/thane/",
        "link_pattern": "lokmat.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "Loksatta Thane",
        "url": "https://www.loksatta.com/thane/",
        "link_pattern": "loksatta.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "ABP Majha Thane (alt)",
        "url": "https://marathi.abplive.com/news/thane",
        "link_pattern": "abplive.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 25
    },
    {
        "name": "Times of India Thane",
        "url": "https://timesofindia.indiatimes.com/city/thane",
        "link_pattern": "timesofindia.indiatimes.com",
        "thane_strict": False,
        "target": 2,
        "fetch_limit": 30
    },
]


# ============================================================
# Google Sheets Setup — with retry
# ============================================================
def setup_google_sheets(max_retries: int = 5, retry_delay: int = 10):
    for attempt in range(1, max_retries + 1):
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file(
                GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=scope
            )
            client = gspread.authorize(creds)

            try:
                sheet = client.open(GOOGLE_SHEET_NAME)
                print(f"✅ Connected: '{GOOGLE_SHEET_NAME}'")
            except gspread.SpreadsheetNotFound:
                sheet = client.create(GOOGLE_SHEET_NAME)
                print(f"✅ Created: '{GOOGLE_SHEET_NAME}'")

            try:
                worksheet = sheet.worksheet(GOOGLE_WORKSHEET_NAME)
                print(f"✅ Worksheet: '{GOOGLE_WORKSHEET_NAME}'")
                current_rows = worksheet.row_count
                if current_rows < 2000:
                    worksheet.add_rows(5000 - current_rows)
                    print(f"✅ Expanded sheet: {current_rows} → 5000 rows")
                else:
                    print(f"✅ Sheet has {current_rows} rows — OK")

            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(
                    title=GOOGLE_WORKSHEET_NAME, rows=5000, cols=10
                )
                worksheet.update('A1:F1', [[
                    'Timestamp (IST)', 'Source', 'Category', 'Title', 'Script', 'Source Link'
                ]])
                worksheet.format('A1:F1', {
                    'textFormat': {
                        'bold': True,
                        'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0}
                    },
                    'backgroundColor': {'red': 0.0, 'green': 0.5, 'blue': 0.25},
                    'horizontalAlignment': 'CENTER'
                })
                worksheet.set_column_width('A', 180)
                worksheet.set_column_width('B', 180)
                worksheet.set_column_width('C', 140)
                worksheet.set_column_width('D', 400)
                worksheet.set_column_width('E', 600)
                worksheet.set_column_width('F', 380)
                print(f"✅ Created worksheet with headers")

            return worksheet

        except gspread.exceptions.APIError as e:
            error_str = str(e)
            if any(code in error_str for code in ['503', '500', '429', '502']):
                if attempt < max_retries:
                    print(f"⚠️ Google Sheets error (attempt {attempt}/{max_retries}) — retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    continue
                else:
                    print(f"❌ Google Sheets unavailable after {max_retries} attempts: {e}")
                    return None
            else:
                print(f"❌ Sheets API error: {e}")
                return None
        except FileNotFoundError:
            print(f"❌ credentials.json not found!")
            return None
        except Exception as e:
            print(f"❌ Sheets setup error: {e}")
            import traceback
            traceback.print_exc()
            return None


# ============================================================
# Save to Google Sheets — with retry + Source column
# ============================================================
def save_to_google_sheets(worksheet, source_name, category, title, script, source_link, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        try:
            timestamp = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')

            script = '\n'.join(str(i) for i in script) if isinstance(script, list) else str(script).strip()
            script = script.replace('[', '').replace(']', '')
            title = str(title).strip()
            source_link = str(source_link).strip()
            category = str(category).strip().lower()

            if category not in VALID_CATEGORIES:
                category = "general"

            next_row = len(worksheet.get_all_values()) + 1
            worksheet.append_row(
                [timestamp, source_name, category.upper(), title, script, source_link],
                value_input_option='RAW'
            )

            worksheet.format(f'A{next_row}:F{next_row}', {
                'textFormat': {
                    'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0},
                    'fontSize': 10
                },
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                'wrapStrategy': 'WRAP',
                'verticalAlignment': 'TOP'
            })

            category_colors = {
                'crime':        {'red': 0.95, 'green': 0.8,  'blue': 0.8},
                'environment':  {'red': 0.8,  'green': 0.95, 'blue': 0.8},
                'development':  {'red': 0.8,  'green': 0.9,  'blue': 1.0},
                'local_events': {'red': 1.0,  'green': 0.9,  'blue': 0.8},
                'health':       {'red': 0.85, 'green': 0.95, 'blue': 0.95},
                'achievement':  {'red': 1.0,  'green': 0.95, 'blue': 0.7},
                'politics':     {'red': 0.9,  'green': 0.85, 'blue': 1.0},
                'general':      {'red': 1.0,  'green': 1.0,  'blue': 0.9},
            }
            worksheet.format(f'C{next_row}', {
                'textFormat': {'bold': True, 'fontSize': 10},
                'backgroundColor': category_colors.get(category, category_colors['general']),
                'horizontalAlignment': 'CENTER'
            })

            print(f"✅ [{source_name[:16]}][{category.upper()}] {title[:45]}...")
            return True

        except gspread.exceptions.APIError as e:
            error_str = str(e)
            if any(code in error_str for code in ['503', '500', '429', '502']):
                if attempt < max_retries:
                    print(f"   ⚠️ Sheets error on save (attempt {attempt}/{max_retries}) — retrying in 8s...")
                    time.sleep(8)
                    continue
                else:
                    print(f"   ❌ Save failed after {max_retries} retries: {e}")
                    return False
            print(f"❌ Save error: {e}")
            return False
        except Exception as e:
            print(f"❌ Save error: {e}")
            return False


# ============================================================
# Helpers
# ============================================================
def get_content_hash(title: str, content: str) -> str:
    return hashlib.md5(
        f"{title.lower()}{content[:200].lower()}".encode()
    ).hexdigest()


def sort_by_count(item):
    return -item[1]


def sort_by_priority(item):
    return {'high': 1, 'medium': 2, 'low': 3}.get(item.get('importance', 'medium'), 2)


def safe_truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    for punct in ['।', '.', '!', '?', '\n']:
        last_pos = truncated.rfind(punct)
        if last_pos > max_chars * 0.7:
            return truncated[:last_pos + 1]
    last_space = truncated.rfind(' ')
    if last_space > max_chars * 0.7:
        return truncated[:last_space]
    return truncated


def get_cta(category: str) -> str:
    return CATEGORY_CTA.get(category.lower().strip(), DEFAULT_CTA)


def is_thane_related(title: str, url: str, content: str = "") -> bool:
    combined = (title + " " + url + " " + content[:500]).lower()
    return any(kw.lower() in combined for kw in THANE_KEYWORDS)


def extract_response_content(response) -> str:
    raw_choice = response.choices[0]
    if hasattr(raw_choice, 'message'):
        msg = raw_choice.message
        if hasattr(msg, 'content') and isinstance(msg.content, str):
            return msg.content
        elif hasattr(msg, 'content') and isinstance(msg.content, list):
            return ' '.join(
                block.get('text', '') if isinstance(block, dict) else str(block)
                for block in msg.content
            )
        elif isinstance(msg, list):
            return ' '.join(
                block.get('text', '') if isinstance(block, dict) else str(block)
                for block in msg
            )
        else:
            return str(msg)
    elif isinstance(raw_choice, dict):
        msg = raw_choice.get('message', {})
        return msg.get('content', '') if isinstance(msg, dict) else str(msg)
    else:
        return str(raw_choice)


def is_script_complete(script: str, category: str = "general") -> bool:
    return "फॉलो करा सलाम ठाणे" in script


def get_last_line(script: str) -> str:
    lines = [l.strip() for l in script.strip().split('\n') if l.strip()]
    return lines[-1] if lines else ""


# ============================================================
# Script Completion Callback
# ============================================================
async def complete_script_if_needed(script: str, news_article: Dict) -> str:
    global total_input_tokens, total_output_tokens, total_cost

    category = news_article.get('category', 'general')
    cta      = get_cta(category)

    if is_script_complete(script, category):
        return script

    last_line = get_last_line(script)
    print(f"   🔧 Script incomplete — last: '{last_line[:60]}' — completing...")

    try:
        response = perplexity_client.chat.completions.create(
            model=SCRIPT_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": f'फक्त मराठी lines लिहा. शेवटची line नक्की हीच: "{cta}"'
                },
                {
                    "role": "user",
                    "content": f"""खालील अर्धवट मराठी script पूर्ण करा.

अर्धवट script:
{script}

नियम:
- वरील script च्या पुढे फक्त उर्वरित lines लिहा
- एकूण 9-14 lines होतील असे नवीन lines जोडा
- शेवटची line नक्की हीच: "{cta}"
- फक्त नवीन lines लिहा, जुन्या lines परत लिहू नका
- फक्त मराठीत लिहा"""
                }
            ],
            temperature=0.7,
            max_tokens=400
        )

        if hasattr(response, 'usage'):
            i_t = response.usage.prompt_tokens
            o_t = response.usage.completion_tokens
            total_input_tokens  += i_t
            total_output_tokens += o_t
            total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

        completion = extract_response_content(response).strip()
        completion = re.sub(r'<think>.*?</think>', '', completion, flags=re.DOTALL).strip()
        completion = completion.replace('```', '').strip()

        if any(kw.lower() in completion.lower() for kw in REFUSAL_KEYWORDS):
            return script.strip() + f"\n\n{cta}"

        completed = script.strip() + "\n\n" + completion.strip()
        if not is_script_complete(completed, category):
            completed = completed.strip() + f"\n\n{cta}"

        print(f"   ✅ Script completed")
        return completed

    except Exception as e:
        error_str = str(e).lower()
        if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
            raise CreditExhaustedException(str(e))
        return script.strip() + f"\n\n{cta}"


# ============================================================
# Marathi Validator
# ============================================================
def is_valid_marathi_script(script: str) -> bool:
    if len(script) < 80:
        return False
    if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
        return False
    devanagari = len(re.findall(r'[\u0900-\u097F]', script))
    total      = len(script.replace(' ', '').replace('\n', ''))
    return (devanagari / max(total, 1)) > 0.35


# ============================================================
# API Credit Check
# ============================================================
async def check_api_credits():
    try:
        perplexity_client.chat.completions.create(
            model=ANALYSIS_MODEL,
            messages=[{"role": "user", "content": "ok"}],
            max_tokens=1
        )
        print("✅ API credits OK")
        return True
    except Exception as e:
        error_str = str(e).lower()
        if any(code in error_str for code in [
            '402', '429', '401', 'insufficient', 'credit',
            'quota', 'balance', 'payment', 'billing', 'rate limit', 'exceeded'
        ]):
            print("=" * 60)
            print("❌ PERPLEXITY API CREDITS EXHAUSTED!")
            print(f"   Error: {str(e)}")
            print("=" * 60)
            print("👉 Top up: https://www.perplexity.ai/settings/api")
            return False
        print(f"❌ Unknown API error: {e}")
        return False


# ============================================================
# Fetch Article with Retry
# ============================================================
async def fetch_article_with_retry(crawler, url: str, retries: int = 3) -> str:
    for attempt in range(1, retries + 1):
        try:
            result = await crawler.arun(
                url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    word_count_threshold=10,
                    page_timeout=25000
                )
            )
            if result.success and len(result.markdown) > 50:
                return result.markdown
            await asyncio.sleep(2)
        except Exception:
            await asyncio.sleep(2)
    return ""


# ============================================================
# Perplexity Fallback — Thane News Search
# Triggered if scraped articles < TARGET_SCRIPTS
# ============================================================
async def fetch_thane_news_via_perplexity(needed: int) -> List[Dict]:
    global total_input_tokens, total_output_tokens, total_cost

    print(f"\n🔁 Fetching {needed} Thane articles via Perplexity fallback...")

    try:
        response = perplexity_client.chat.completions.create(
            model=ANALYSIS_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a Thane district news researcher. Return ONLY valid JSON array."
                },
                {
                    "role": "user",
                    "content": f"""Find the latest {needed} news articles specifically about Thane district, Maharashtra.
Focus areas: Thane city, Dombivli, Kalyan, Bhiwandi, Ulhasnagar, Ambernath, Badlapur, Mira Road, Mumbra.

Return JSON array with {needed} items:
[{{
  "title": "News headline",
  "detailed_summary": "100-150 word Marathi summary",
  "category": "crime/environment/development/local_events/health/achievement/politics/general",
  "importance": "high/medium/low",
  "key_points": ["point1 in Marathi", "point2 in Marathi", "point3 in Marathi"],
  "link": "source url or empty string"
}}]
Rules:
- ALL news must be from Thane district only
- detailed_summary and key_points in Marathi
- Return only JSON array"""
                }
            ],
            temperature=0.3,
            max_tokens=3500
        )

        if hasattr(response, 'usage'):
            i_t = response.usage.prompt_tokens
            o_t = response.usage.completion_tokens
            total_input_tokens  += i_t
            total_output_tokens += o_t
            c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
            total_cost += c
            print(f"   📊 {i_t}in + {o_t}out = ${c:.4f}")

        content = extract_response_content(response)
        content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
        match = re.search(r'\[.*\]', content, re.DOTALL)

        if match:
            parsed = json.loads(match.group())
            results = []
            for art in parsed:
                if art.get('category') not in VALID_CATEGORIES:
                    art['category'] = 'general'
                art['source']     = 'Perplexity Fallback'
                art['scraped_at'] = datetime.now(IST).isoformat()
                art['hash']       = get_content_hash(
                    art.get('title', ''), art.get('detailed_summary', '')
                )
                results.append(art)
            print(f"   ✅ Got {len(results)} Thane articles from Perplexity")
            return results
        return []

    except Exception as e:
        error_str = str(e).lower()
        if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient']):
            raise CreditExhaustedException(str(e))
        print(f"   ❌ Perplexity fallback error: {e}")
        return []


# ============================================================
# Scraping — 12 Thane Sources
# ============================================================
async def scrape_thane_news_sources() -> List[Dict]:
    all_news = []

    async with AsyncWebCrawler(verbose=False) as crawler:
        for site in NEWS_SITES:
            print(f"\n{'='*60}")
            print(f"🔍 {site['name']} | Target: {site['target']}")
            print(f"{'='*60}")

            site_articles = []

            try:
                result = await crawler.arun(
                    site['url'],
                    config=CrawlerRunConfig(
                        cache_mode=CacheMode.BYPASS,
                        wait_for="body",
                        word_count_threshold=10,
                        page_timeout=30000,
                        js_code="await new Promise(r => setTimeout(r, 3000));"
                    )
                )

                if not result.success:
                    print(f"❌ Failed to load: {site['name']}")
                    continue

                soup = BeautifulSoup(result.html, 'html.parser')
                raw_articles = []

                for link_tag in soup.find_all('a', href=True):
                    href  = link_tag.get('href', '')
                    title = link_tag.get_text(strip=True)

                    if any(kw.lower() in title.lower() for kw in SKIP_TITLE_KEYWORDS):
                        continue
                    if any(x in href.lower() for x in SKIP_URL_PATTERNS):
                        continue
                    if not (15 < len(title) < 300):
                        continue
                    if site['link_pattern'] not in href:
                        continue

                    # Strict Thane filter for national/general sources
                    if site.get('thane_strict', False):
                        if not is_thane_related(title, href):
                            continue

                    if href.startswith('/'):
                        base = site['url'].split('/') + '//' + site['url'].split('/')[2]
                        href = base + href

                    if href.startswith('http'):
                        raw_articles.append({'title': title, 'link': href})

                seen = set()
                unique_links = []
                for a in raw_articles:
                    if a['link'] not in seen:
                        unique_links.append(a)
                        seen.add(a['link'])

                print(f"📋 Found {len(unique_links)} unique links")

                for article in unique_links:
                    if len(site_articles) >= site['target']:
                        break

                    print(f"   🔗 [{len(site_articles)+1}/{site['target']}] {article['title'][:50]}...")

                    markdown = await fetch_article_with_retry(crawler, article['link'])
                    content  = markdown if markdown else article['title']

                    # Extra Thane check on full article content
                    if site.get('thane_strict', False):
                        if not is_thane_related(article['title'], article['link'], content):
                            print(f"   🚫 Not Thane-related — skipped")
                            continue

                    if any(kw.lower() in content.lower() for kw in SKIP_CONTENT_KEYWORDS):
                        print(f"   ⏭️  Skipped (utility/spiritual content)")
                        continue

                    content_hash = get_content_hash(article['title'], content)

                    if content_hash not in processed_hashes:
                        site_articles.append({
                            'title':            article['title'],
                            'link':             article['link'],
                            'content':          safe_truncate(content, 3500),
                            'hash':             content_hash,
                            'has_full_content': bool(markdown)
                        })
                        processed_hashes.add(content_hash)
                        tag = "✅" if markdown else "⚠️ fallback"
                        print(f"   {tag} [{len(site_articles)}/{site['target']}] {article['title'][:50]}...")
                    else:
                        print(f"   🔄 Duplicate skipped")

                    await asyncio.sleep(1)

                print(f"\n📦 {site['name']}: {len(site_articles)}/{site['target']} collected")

                if site_articles:
                    filtered = await smart_analyze_with_category(site_articles, site['name'])
                    all_news.extend(filtered)
                    print(f"🧠 {site['name']}: Analyzed {len(filtered)} articles")

            except CreditExhaustedException:
                raise
            except Exception as e:
                print(f"❌ Error {site['name']}: {e}")

            await asyncio.sleep(2)

    return all_news


# ============================================================
# AI Categorization — Thane-Specific
# ============================================================
async def smart_analyze_with_category(articles: List[Dict], source_name: str):
    global total_input_tokens, total_output_tokens, total_cost

    all_filtered = []

    for batch_start in range(0, len(articles), 5):
        raw_batch = articles[batch_start:batch_start + 5]

        batch = [
            article for article in raw_batch
            if not any(kw.lower() in article.get('content', '').lower()
                       for kw in SKIP_CONTENT_KEYWORDS)
        ]

        if not batch:
            continue

        index_to_link  = {i: article['link']  for i, article in enumerate(batch)}
        index_to_title = {i: article['title'] for i, article in enumerate(batch)}

        articles_text = ""
        for idx, article in enumerate(batch):
            articles_text += f"INDEX_{idx}: {article['title']}\n{safe_truncate(article['content'], 500)}\n---\n"

        prompt = f"""ठाणे जिल्ह्यातील मराठी बातम्या विश्लेषक: खालील बातम्यांना category आणि Marathi summary द्या.

⚠️ नियम:
1. detailed_summary आणि key_points फक्त मराठीत लिहा
2. JSON मध्ये "index" field EXACTLY जसा दिला (0,1,2,3,4) तसाच परत द्या
3. फक्त ठाणे जिल्ह्याशी थेट संबंधित बातम्यांना "high" importance द्या

Categories (फक्त यापैकी एक):
- crime → गुन्हे, अपघात, मृत्यू, अटक
- environment → झाडे, प्रदूषण, वनवा, उद्यान
- development → मेट्रो, रस्ते, पूल, TMC प्रकल्प
- local_events → उत्सव, रॅली, स्पर्धा, सोहळे
- health → आरोग्य सेवा, रुग्णालय, योजना
- achievement → विक्रम, पुरस्कार, यश
- politics → राजकारण, निवडणूक, आंदोलन
- general → इतर सर्व

JSON array format:
[{{"index": 0, "category": "cat", "detailed_summary": "मराठी सारांश १००-१५० शब्द", "importance": "high/medium/low", "key_points": ["मुद्दा १", "मुद्दा २", "मुद्दा ३"]}}]

बातम्या:
{articles_text}

फक्त JSON array. Index 0 ते {len(batch)-1} पर्यंत."""

        try:
            response = perplexity_client.chat.completions.create(
                model=ANALYSIS_MODEL,
                messages=[
                    {"role": "system", "content": "Return ONLY valid JSON array."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=3000
            )

            if hasattr(response, 'usage'):
                i_t = response.usage.prompt_tokens
                o_t = response.usage.completion_tokens
                total_input_tokens  += i_t
                total_output_tokens += o_t
                c = (i_t * ANALYSIS_INPUT_COST) + (o_t * ANALYSIS_OUTPUT_COST)
                total_cost += c
                print(f"   📊 {i_t}in + {o_t}out = ${c:.4f}")

            content = extract_response_content(response)
            if not content.strip():
                raise ValueError("Empty API response")

            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            match = re.search(r'\[.*\]', content, re.DOTALL)

            if match:
                parsed = json.loads(match.group())
                for art in parsed:
                    idx = art.get('index')
                    if idx is not None and idx in index_to_link:
                        art['link']  = index_to_link[idx]
                        art['title'] = index_to_title[idx]
                    else:
                        pos = len(all_filtered) % len(batch)
                        art['link']  = index_to_link.get(pos, '')
                        art['title'] = index_to_title.get(pos, art.get('title', ''))

                    if art.get('category') not in VALID_CATEGORIES:
                        art['category'] = 'general'

                all_filtered.extend(parsed)
                print(f"   ✅ Categorized {len(parsed)}")
            else:
                for i, article in enumerate(batch):
                    all_filtered.append({
                        'index':            i,
                        'title':            article['title'],
                        'category':         'general',
                        'detailed_summary': safe_truncate(article['content'], 600),
                        'importance':       'medium',
                        'link':             article['link'],
                        'key_points':       [article['title']]
                    })

        except json.JSONDecodeError:
            for i, article in enumerate(batch):
                all_filtered.append({
                    'index':            i,
                    'title':            article['title'],
                    'category':         'general',
                    'detailed_summary': safe_truncate(article['content'], 600),
                    'importance':       'medium',
                    'link':             article['link'],
                    'key_points':       [article['title']]
                })

        except Exception as e:
            error_str = str(e).lower()
            if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
                raise CreditExhaustedException(str(e))
            print(f"   ❌ AI error: {e}")
            for i, article in enumerate(batch):
                all_filtered.append({
                    'index':            i,
                    'title':            article['title'],
                    'category':         'general',
                    'detailed_summary': safe_truncate(article['content'], 600),
                    'importance':       'medium',
                    'link':             article['link'],
                    'key_points':       [article['title'], safe_truncate(article['content'], 100)]
                })

        await asyncio.sleep(1.5)

    for art in all_filtered:
        art['source']     = source_name
        art['scraped_at'] = datetime.now(IST).isoformat()

    return all_filtered


# ============================================================
# Script Generation — Salaam Thane Style
# ============================================================
async def create_reel_script_single(news_article: Dict) -> str:
    global total_input_tokens, total_output_tokens, total_cost

    category = news_article.get('category', 'general')
    cta      = get_cta(category)

    hook_templates = {
        "crime":        "ज्या वयात [innocence]... त्याच वयात [tragedy]... (emotional contrast)",
        "environment":  "ठाणेकरांनो सावधान — [problem statement] (direct alarm hook)",
        "development":  "[benefit] येणार, पण त्याची डार्क साईड माहिती आहे? (twist hook)",
        "local_events": "[visual scene: outfit/setting/people]. ठाण्याच्या [audience group] तयार व्हा! (scene-setting hook)",
        "health":       "आता [service/facility] होणार स्मार्ट — [location] मध्ये [benefit]. (benefit-first hook)",
        "achievement":  "[visual scene]. जेव्हा [action] होतो, तेव्हा काय होतं माहिती आहे? [reveal]. (reveal hook)",
        "politics":     "ठाण्यातील राजकारण आता [adjective] झालंय... (drama hook)",
        "general":      "ठाणे जिल्ह्यात एक [adjective] घटना घडली आहे... (direct hook)",
    }
    hook_guide = hook_templates.get(category, hook_templates["general"])

    system_prompt = f"""तुम्ही "सलाम ठाणे" या Instagram channel चे मराठी Reel script writer आहात.
फक्त मराठी भाषेत लिहा. तुम्ही AI आहात हे कधीही सांगू नका. फक्त script लिहा.

🎯 Channel Focus: फक्त ठाणे जिल्ह्यातील hyperlocal बातम्या
📍 Local areas: ठाणे, डोंबिवली, कल्याण, भिवंडी, उल्हासनगर, अंबरनाथ, मीरा रोड, बदलापूर, पारसिक, मुंब्रा, कोपरी

Script Structure (9-14 lines total):
- Line 1-2: HOOK — {hook_guide}
- Line 3-9: सर्व facts — नावे, ठिकाण, तारीख, अचूक संख्या सह
- Line 10-12: प्रश्न / विश्लेषण / twist
- Line 13-14: audience question + CTA

✅ MUST use at least 3 of these Salaam Thane signature phrases:
→ "तब्बल [number]" — for shocking stats
→ "विशेष म्हणजे" — to flag a key highlight
→ "सर्वात महत्त्वाचं म्हणजे" — for the main point
→ "पण इथेच खरा प्रश्न उभा राहतो" — for controversy/twist
→ "अक्षरशः" — for dramatic emphasis
→ "सर्वात चीड यायची गोष्ट म्हणजे" — for outrage (crime/injustice)
→ "आपल्या [ठाण्याच्या/डोंबिवलीच्या/कल्याणच्या]..." — local ownership
→ "कौतुक आहे [person/institution] चं" — for positive stories

कठोर नियम:
- 9-14 lines total, प्रत्येक line 1-2 sentences
- संपूर्ण output फक्त मराठीत (proper nouns सोडून)
- कोणतेही heading, explanation, markdown नाही
- "माहिती नाही", "खेद आहे", "मी Perplexity" असे कधीही लिहू नका
- शेवटची line नक्की हीच: "{cta}"
- script अर्धवट सोडू नका — शेवटपर्यंत लिहा"""

    summary    = safe_truncate(news_article.get('detailed_summary', news_article.get('title', '')), 600)
    key_points = ', '.join(news_article.get('key_points', [news_article.get('title', '')]))

    user_prompt_v1 = f"""Category: {category.upper()}
शीर्षक: {news_article['title']}
सारांश: {summary}
मुद्दे: {key_points}

वरील ठाणे जिल्ह्यातील बातमीचे facts वापरून 9-14 मराठी lines तयार करा.
"तब्बल", "विशेष म्हणजे", "सर्वात महत्त्वाचं म्हणजे" यापैकी किमान 3 phrases वापरा.
शेवटची line नक्की: "{cta}"
जरी माहिती कमी असली तरी उपलब्ध तथ्यांवर आधारित पूर्ण script लिहा."""

    user_prompt_v2 = f"""खालील ठाणे जिल्ह्यातील बातमीवर 10 मराठी वाक्ये लिहा.
बातमी: {news_article['title']}. {safe_truncate(summary, 200)}
- प्रत्येक वाक्य नवीन line वर
- फक्त मराठीत
- "तब्बल" किंवा "विशेष म्हणजे" नक्की वापरा
- शेवटची line नक्की: "{cta}" """

    for attempt in range(1, 3):
        try:
            response = perplexity_client.chat.completions.create(
                model=SCRIPT_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt_v1 if attempt == 1 else user_prompt_v2}
                ],
                temperature=0.8,
                max_tokens=1500
            )

            if hasattr(response, 'usage'):
                i_t = response.usage.prompt_tokens
                o_t = response.usage.completion_tokens
                total_input_tokens  += i_t
                total_output_tokens += o_t
                total_cost += (i_t * SCRIPT_INPUT_COST) + (o_t * SCRIPT_OUTPUT_COST)

            script = extract_response_content(response).strip()
            script = re.sub(r'<think>.*?</think>', '', script, flags=re.DOTALL).strip()
            script = script.replace('```', '').strip()

            if any(kw.lower() in script.lower() for kw in REFUSAL_KEYWORDS):
                print(f"   ⚠️ Attempt {attempt}: Refusal — retrying...")
                continue

            if is_valid_marathi_script(script):
                return await complete_script_if_needed(script, news_article)

            print(f"   ⚠️ Attempt {attempt}: Not valid Marathi — retrying...")

        except CreditExhaustedException:
            raise
        except Exception as e:
            error_str = str(e).lower()
            if any(code in error_str for code in ['402', '429', 'credit', 'quota', 'insufficient', 'balance', 'billing']):
                raise CreditExhaustedException(str(e))
            print(f"   ⚠️ Attempt {attempt} error: {e}")
            await asyncio.sleep(2)

    # Fallback script
    title_fb   = news_article.get('title', 'एक महत्त्वाची बातमी')[:80]
    summary_fb = safe_truncate(news_article.get('detailed_summary', news_article.get('title', '')), 200)
    cta_fb     = get_cta(category)

    return f"""{title_fb}

{summary_fb}

ही घटना ठाणे जिल्ह्यात मोठी चर्चा निर्माण करत आहे.

विशेष म्हणजे या प्रकरणात अनेक महत्त्वाचे प्रश्न उपस्थित होत आहेत.

सर्वात महत्त्वाचं म्हणजे सर्वसामान्य ठाणेकरांवर याचा थेट परिणाम होणार आहे.

पण इथेच खरा प्रश्न उभा राहतो — प्रशासन नक्की काय पाऊल उचलणार?

{cta_fb}"""


# ============================================================
# Main Pipeline
# ============================================================
async def main():
    global total_input_tokens, total_output_tokens, total_cost

    print("=" * 70)
    print("🟢 SALAAM THANE — HYPERLOCAL REEL SCRIPT GENERATOR v2.0")
    print(f"🔍 Analysis : {ANALYSIS_MODEL}")
    print(f"✍️  Scripts  : {SCRIPT_MODEL}")
    print(f"🎯 Target   : {TARGET_SCRIPTS} scripts")
    print(f"📍 Focus    : ठाणे जिल्हा — 12 Sources")
    print(f"🕐 Timezone : IST (Asia/Kolkata)")
    print("=" * 70)

    credits_ok = await check_api_credits()
    if not credits_ok:
        print("\n🛑 Stopping. Top up credits first.")
        print("👉 https://www.perplexity.ai/settings/api")
        return

    start_time = datetime.now(IST)

    print("\n" + "=" * 70)
    print("STEP 1: SCRAPING 12 THANE NEWS SOURCES")
    print("=" * 70 + "\n")

    try:
        all_articles = await scrape_thane_news_sources()
    except CreditExhaustedException:
        print("\n🛑 Credits exhausted during scraping. Stopping.")
        return

    # De-duplicate
    unique_articles = []
    seen_hashes = set()
    for article in all_articles:
        h = article.get('hash', get_content_hash(
            article['title'], article.get('detailed_summary', '')
        ))
        if h not in seen_hashes:
            unique_articles.append(article)
            seen_hashes.add(h)

    print(f"\n✅ Total unique Thane articles: {len(unique_articles)}")

    # Perplexity fallback if not enough
    if len(unique_articles) < TARGET_SCRIPTS:
        needed = TARGET_SCRIPTS - len(unique_articles)
        print(f"⚡ Only {len(unique_articles)} scraped — fetching {needed} more via Perplexity...")
        try:
            extra = await fetch_thane_news_via_perplexity(needed)
            for art in extra:
                h = art.get('hash', '')
                if h not in seen_hashes:
                    unique_articles.append(art)
                    seen_hashes.add(h)
        except CreditExhaustedException:
            print("🛑 Credits exhausted during Perplexity fallback.")

    # Category breakdown
    category_counts = {}
    for article in unique_articles:
        cat = article.get('category', 'general')
        category_counts[cat] = category_counts.get(cat, 0) + 1

    print("\n📊 Category Breakdown:")
    for cat, count in sorted(category_counts.items(), key=sort_by_count):
        print(f"   {cat.upper():<15} {'█' * count} ({count})")

    unique_articles.sort(key=sort_by_priority)
    selected_articles = unique_articles[:TARGET_SCRIPTS]

    print(f"\n🎯 Selected : {len(selected_articles)}/{TARGET_SCRIPTS}")
    print(f"⏱️  Scraping : {(datetime.now(IST)-start_time).total_seconds():.0f}s\n")

    print("=" * 70)
    print("STEP 2: GENERATING SCRIPTS → GOOGLE SHEETS")
    print("=" * 70 + "\n")

    worksheet        = setup_google_sheets()
    successful_saves = 0
    failed_saves     = 0

    if not worksheet:
        print("❌ Cannot connect to Google Sheets. Aborting.")
        return

    for idx, article in enumerate(selected_articles, 1):
        cat = article.get('category', 'general')
        src = article.get('source', '')[:16]
        print(f"\n[{idx:02d}/{len(selected_articles)}] {src:<16} | {cat.upper():<13} | {article['title'][:40]}...")

        try:
            script = await create_reel_script_single(article)
        except CreditExhaustedException:
            print(f"\n🛑 Credits exhausted at script {idx}/{len(selected_articles)}")
            print(f"   ✅ Saved so far: {successful_saves} scripts")
            print(f"👉 Top up: https://www.perplexity.ai/settings/api")
            break

        dev_chars   = len(re.findall(r'[\u0900-\u097F]', script))
        total_ch    = len(script.replace(' ', '').replace('\n', ''))
        marathi_pct = (dev_chars / max(total_ch, 1)) * 100
        lang_tag    = "🇮🇳" if marathi_pct > 35 else "⚠️"
        cta_tag     = "✅" if is_script_complete(script, cat) else "⚠️ NO CTA"
        print(f"   📝 {lang_tag} ({marathi_pct:.0f}%) | CTA:{cta_tag} | 🔗 {article.get('link','')[:55]}...")

        success = save_to_google_sheets(
            worksheet,
            article.get('source', 'Unknown'),
            cat,
            article['title'],
            script,
            article.get('link', '')
        )
        if success:
            successful_saves += 1
        else:
            failed_saves += 1

        await asyncio.sleep(1)

    total_duration = (datetime.now(IST) - start_time).total_seconds()
    total_tokens   = total_input_tokens + total_output_tokens

    print("\n" + "=" * 70)
    print("📈 FINAL SUMMARY — SALAAM THANE v2.0")
    print("=" * 70)
    print(f"   🔍 Analysis model     : {ANALYSIS_MODEL}")
    print(f"   ✍️  Script model       : {SCRIPT_MODEL}")
    print(f"   📰 Sources used       : {len(NEWS_SITES)} sites")
    print(f"   📰 Articles scraped   : {len(unique_articles)}")
    print(f"   ✅ Scripts saved      : {successful_saves}")
    print(f"   ❌ Failed             : {failed_saves}")
    print(f"   ⏱️  Total time         : {total_duration:.0f}s ({total_duration/60:.1f} mins)")
    print(f"   📥 Input tokens       : {total_input_tokens:,}")
    print(f"   📤 Output tokens      : {total_output_tokens:,}")
    print(f"   🔢 Total tokens       : {total_tokens:,}")
    print(f"   💰 Total cost         : ${total_cost:.4f} (~₹{total_cost*84:.2f})")
    print(f"   💵 Cost per script    : ${total_cost/max(successful_saves,1):.4f}")
    print(f"   🕐 Finished at (IST)  : {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    if worksheet:
        print(f"   📊 Sheet URL          : https://docs.google.com/spreadsheets/d/"
              f"{worksheet.spreadsheet.id}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    asyncio.run(main())