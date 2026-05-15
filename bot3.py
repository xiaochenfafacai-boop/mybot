import logging
import sqlite3
import json
from datetime import datetime
import pytz
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
import re
import io
import csv
import threading
from flask import Flask, request, jsonify
import os

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# ========== 配置 ==========
TOKEN = "8885640450:AAGwPtneqg_VF5l1GAuoKa4Ojz5JS9yGuuM"
MASTER_USER_ID = 8782394486
WEB_URL = "https://mybot-7tyh.onrender.com"
PORT = int(os.environ.get('PORT', 8080))

TIMEZONES = {
    'china': 'Asia/Shanghai',
    'myanmar': 'Asia/Yangon',
    'thailand': 'Asia/Bangkok',
}

flask_app = Flask(__name__)

# ========== 数据库函数 ==========

def get_current_time(timezone_str):
    try:
        tz = pytz.timezone(timezone_str)
        now = datetime.now(tz)
        return now, now.strftime("%H:%M:%S"), now.strftime("%Y-%m-%d %H:%M:%S")
    except:
        tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(tz)
        return now, now.strftime("%H:%M:%S"), now.strftime("%Y-%m-%d %H:%M:%S")

def init_db():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (group_id INTEGER PRIMARY KEY,
                  operators TEXT DEFAULT '[]',
                  exchange_rate REAL DEFAULT 7.2,
                  fee_rate REAL DEFAULT 0,
                  is_active INTEGER DEFAULT 0,
                  language TEXT DEFAULT 'chinese',
                  timezone TEXT DEFAULT 'Asia/Shanghai',
                  show_usdt INTEGER DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS bills
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  group_id INTEGER,
                  user_id INTEGER,
                  username TEXT,
                  remark TEXT,
                  amount REAL,
                  usdt_amount REAL,
                  exchange_rate REAL,
                  bill_type TEXT,
                  timestamp TEXT)''')
    conn.commit()
    conn.close()
    print("✅ 数据库初始化完成")

def get_setting(group_id, key):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT * FROM settings WHERE group_id = ?", (group_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    cols = ['group_id', 'operators', 'exchange_rate', 'fee_rate', 'is_active', 'language', 'timezone', 'show_usdt']
    return dict(zip(cols, row)).get(key)

def update_setting(group_id, key, value):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT * FROM settings WHERE group_id = ?", (group_id,))
    if c.fetchone():
        c.execute(f"UPDATE settings SET {key} = ? WHERE group_id = ?", (value, group_id))
    else:
        c.execute("INSERT INTO settings (group_id, operators, exchange_rate, fee_rate, is_active, language, timezone, show_usdt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (group_id, '[]', 7.2, 0, 0, 'chinese', 'Asia/Shanghai', 1))
        c.execute(f"UPDATE settings SET {key} = ? WHERE group_id = ?", (value, group_id))
    conn.commit()
    conn.close()

def is_master(user_id):
    return user_id == MASTER_USER_ID

def is_operator(group_id, user_id):
    ops = json.loads(get_setting(group_id, 'operators') or '[]')
    return user_id in ops

def can_use(group_id, user_id):
    return is_master(user_id) or is_operator(group_id, user_id)

def add_bill(group_id, user_id, username, remark, amount, bill_type, exchange_rate=None):
    if exchange_rate is None:
        exchange_rate = get_setting(group_id, 'exchange_rate') or 7.2
    if bill_type == 'income':
        usdt_amount = amount / exchange_rate
    else:
        usdt_amount = amount
    tz_str = get_setting(group_id, 'timezone') or 'Asia/Shanghai'
    _, _, full_time = get_current_time(tz_str)
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''INSERT INTO bills 
                 (group_id, user_id, username, remark, amount, usdt_amount, exchange_rate, bill_type, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (group_id, user_id, username, remark, amount, usdt_amount, exchange_rate, bill_type, full_time))
    conn.commit()
    conn.close()
    return usdt_amount

def get_today_bills(group_id):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    tz_str = get_setting(group_id, 'timezone') or 'Asia/Shanghai'
    now, _, _ = get_current_time(tz_str)
    today_date = now.strftime("%Y-%m-%d")
    c.execute("SELECT remark, username, amount, usdt_amount, exchange_rate, timestamp FROM bills WHERE group_id = ? AND bill_type = 'income' AND date(timestamp) = ? ORDER BY id DESC", (group_id, today_date))
    income = c.fetchall()
    c.execute("SELECT remark, username, usdt_amount, exchange_rate, timestamp FROM bills WHERE group_id = ? AND bill_type = 'expense' AND date(timestamp) = ? ORDER BY id DESC", (group_id, today_date))
    expense = c.fetchall()
    c.execute("SELECT SUM(amount), SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'income' AND date(timestamp) = ?", (group_id, today_date))
    total_income = c.fetchone()
    c.execute("SELECT SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'expense' AND date(timestamp) = ?", (group_id, today_date))
    total_expense = c.fetchone()
    conn.close()
    return income, expense, total_income, total_expense, today_date

def get_bills_by_date(group_id, date_str):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT remark, username, amount, usdt_amount, exchange_rate, bill_type, timestamp FROM bills WHERE group_id = ? AND date(timestamp) = ? ORDER BY timestamp DESC", 
              (group_id, date_str))
    bills = c.fetchall()
    c.execute("SELECT SUM(amount), SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'income' AND date(timestamp) = ?", 
              (group_id, date_str))
    total_income = c.fetchone()
    c.execute("SELECT SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'expense' AND date(timestamp) = ?", 
              (group_id, date_str))
    total_expense = c.fetchone()
    conn.close()
    return bills, total_income, total_expense

def delete_today_bills(group_id):
    tz_str = get_setting(group_id, 'timezone') or 'Asia/Shanghai'
    now, _, _ = get_current_time(tz_str)
    today_date = now.strftime("%Y-%m-%d")
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("DELETE FROM bills WHERE group_id = ? AND date(timestamp) = ?", (group_id, today_date))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    return deleted

def delete_last_bill(group_id):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT id FROM bills WHERE group_id = ? ORDER BY id DESC LIMIT 1", (group_id,))
    last = c.fetchone()
    if last:
        c.execute("DELETE FROM bills WHERE id = ?", (last[0],))
        deleted = 1
    else:
        deleted = 0
    conn.commit()
    conn.close()
    return deleted

def delete_all_bills(group_id):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("DELETE FROM bills WHERE group_id = ?", (group_id,))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    return deleted

def delete_user_bills(group_id, name):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("DELETE FROM bills WHERE group_id = ? AND (LOWER(username) = ? OR LOWER(remark) = ?)", (group_id, name.lower(), name.lower()))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    return deleted

# ========== 分类统计 ==========

def get_remark_stats(group_id, date_str):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT remark, COUNT(*), SUM(amount), SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'income' AND date(timestamp) = ? GROUP BY remark ORDER BY SUM(usdt_amount) DESC", 
              (group_id, date_str))
    stats = c.fetchall()
    conn.close()
    return stats

def get_operator_stats(group_id, date_str):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT username, COUNT(*), SUM(amount), SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'income' AND date(timestamp) = ? GROUP BY username ORDER BY SUM(usdt_amount) DESC", 
              (group_id, date_str))
    stats = c.fetchall()
    conn.close()
    return stats

# ========== CSV 导出 ==========

async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE, gid=None):
    if gid is None:
        gid = update.effective_chat.id
    
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT remark, username, amount, usdt_amount, exchange_rate, timestamp FROM bills WHERE group_id = ? AND bill_type = 'income' ORDER BY timestamp DESC", (gid,))
    bills = c.fetchall()
    c.execute("SELECT remark, username, usdt_amount, timestamp FROM bills WHERE group_id = ? AND bill_type = 'expense' ORDER BY timestamp DESC", (gid,))
    expenses = c.fetchall()
    c.execute("SELECT SUM(amount), SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'income'", (gid,))
    total_income = c.fetchone()
    c.execute("SELECT SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'expense'", (gid,))
    total_expense = c.fetchone()
    conn.close()
    
    rate = get_setting(gid, 'exchange_rate') or 7.2
    total_rmb = total_income[0] or 0
    total_usdt = total_income[1] or 0
    expense_usdt = total_expense[0] or 0
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['日期', '时间', '备注', '操作人', '金额(元)', '汇率', 'USDT'])
    for bill in bills:
        remark, username, amount, usdt, ex_rate, ts = bill
        time_str = ts[11:16] if len(ts) > 11 else ts
        date_str = ts[5:10] if len(ts) > 10 else ts
        writer.writerow([date_str, time_str, remark or '-', username, f"{amount:.0f}", f"{ex_rate:.2f}", f"{usdt:.2f}"])
    writer.writerow([])
    writer.writerow(['汇总'])
    writer.writerow(['总入款(元)', f"{total_rmb:.0f}"])
    writer.writerow(['总入款(USDT)', f"{total_usdt:.2f}"])
    writer.writerow(['已下发(USDT)', f"{expense_usdt:.2f}"])
    writer.writerow(['未下发(USDT)', f"{total_usdt - expense_usdt:.2f}"])
    writer.writerow(['当前汇率', f"{rate:.2f}"])
    
    output.seek(0)
    today = datetime.now().strftime("%Y%m%d")
    
    if update.callback_query:
        await update.callback_query.message.reply_document(
            document=io.BytesIO(output.getvalue().encode('utf-8-sig')),
            filename=f"账单_{today}.csv",
            caption=f"📊 账单导出\n总入款: {total_rmb:.0f} 元 = {total_usdt:.2f} U"
        )
    else:
        await update.message.reply_document(
            document=io.BytesIO(output.getvalue().encode('utf-8-sig')),
            filename=f"账单_{today}.csv",
            caption=f"📊 账单导出\n总入款: {total_rmb:.0f} 元 = {total_usdt:.2f} U"
        )

# ========== Web ==========

@flask_app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>记账账单</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; background: #f0f2f5; padding: 20px; }
            .container { max-width: 1400px; margin: 0 auto; background: white; border-radius: 16px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); overflow: hidden; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 24px 30px; }
            .header h1 { font-size: 28px; margin-bottom: 8px; }
            .date-nav { background: white; padding: 15px 20px; border-bottom: 1px solid #e0e0e0; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; }
            .date-nav button { background: #667eea; color: white; border: none; padding: 8px 20px; border-radius: 8px; cursor: pointer; }
            .date-picker { display: flex; gap: 10px; align-items: center; }
            .date-picker input { padding: 8px 12px; border: 1px solid #ddd; border-radius: 8px; }
            .content { padding: 24px 30px; }
            .section { margin-bottom: 32px; }
            .section-title { font-size: 18px; font-weight: 600; margin-bottom: 16px; padding-bottom: 8px; border-bottom: 2px solid #667eea; }
            table { width: 100%; border-collapse: collapse; font-size: 14px; }
            th, td { padding: 12px 10px; text-align: left; border-bottom: 1px solid #eef2f6; }
            th { background: #f8f9fc; font-weight: 600; }
            .stats-box { background: linear-gradient(135deg, #f8f9fc 0%, #f0f2f5 100%); border-radius: 12px; padding: 24px; margin-top: 20px; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; }
            .stat-card { background: white; padding: 16px; border-radius: 12px; text-align: center; }
            .stat-label { font-size: 12px; color: #888; margin-bottom: 8px; }
            .stat-value { font-size: 24px; font-weight: 700; color: #333; }
            .stat-list { background: white; padding: 16px; border-radius: 12px; margin-bottom: 16px; }
            .stat-item { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #eef2f6; }
            .stat-name { font-weight: 500; color: #333; }
            .stat-number { color: #667eea; font-weight: 600; }
            .loading { text-align: center; padding: 50px; color: #888; }
            .footer { background: #f8f9fc; padding: 16px 30px; text-align: center; font-size: 12px; color: #888; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header"><h1>📋 记账账单</h1><p id="dateInfo">加载中...</p></div>
            <div class="date-nav">
                <div class="date-picker"><label>📅 选择日期：</label><input type="date" id="datePicker" onchange="loadData()"><button onclick="loadData()">跳转</button></div>
                <div class="date-picker"><button onclick="prevDay()">◀ 前一天</button><button onclick="nextDay()">后一天 ▶</button></div>
            </div>
            <div class="content" id="content"><div class="loading">加载中...</div></div>
            <div class="footer"><p>💡 选择日期查看任意一天的账单</p></div>
        </div>
        <script>
            let currentDate = new Date().toISOString().split('T')[0];
            let GROUP_ID = null;
            
            function getDateFromURL() {
                const urlParams = new URLSearchParams(window.location.search);
                const date = urlParams.get('date');
                if (date) { currentDate = date; document.getElementById('datePicker').value = date; }
                GROUP_ID = urlParams.get('group_id');
                if (!GROUP_ID) {
                    document.getElementById('content').innerHTML = '<div class="loading">❌ 请通过机器人的"查看完整账单"按钮访问</div>';
                    return false;
                }
                return true;
            }
            
            async function loadData() {
                if (!GROUP_ID) { return; }
                const datePicker = document.getElementById('datePicker');
                currentDate = datePicker.value;
                document.getElementById('dateInfo').innerHTML = `📅 ${currentDate} | 时差对照：UTC+8 北京时间`;
                document.getElementById('content').innerHTML = '<div class="loading">加载中...</div>';
                try {
                    const response = await fetch(`/api/bill?date=${currentDate}&group_id=${GROUP_ID}`);
                    const data = await response.json();
                    if (data.error) {
                        document.getElementById('content').innerHTML = '<div class="loading">暂无账单数据</div>';
                        return;
                    }
                    let html = '';
                    
                    if (data.income_bills && data.income_bills.length > 0) {
                        html += `<div class="section"><div class="section-title">📥 入款记录 (${data.income_bills.length} 笔)</div>
                            <table><thead><tr><th>备注</th><th>时间</th><th>金额(元)</th><th>汇率</th><th>USDT</th><th>操作人</th></tr></thead><tbody>`;
                        for (const bill of data.income_bills) {
                            html += `<tr>
                                <td>${bill.remark || '-'}</td>
                                <td>${bill.time}</td>
                                <td>${bill.amount}</td>
                                <td>${bill.exchange_rate}</td>
                                <td>${bill.usdt}${bill.show_usdt ? 'U' : ''}</td>
                                <td>${bill.username}</td>
                            </tr>`;
                        }
                        html += `</tbody></table></div>`;
                    } else {
                        html += `<div class="section"><div class="section-title">📥 入款记录</div><div class="loading">暂无入款记录</div></div>`;
                    }
                    
                    if (data.expense_bills && data.expense_bills.length > 0) {
                        html += `<div class="section"><div class="section-title">📤 下发记录 (${data.expense_bills.length} 笔)</div>
                            </table><thead><tr><th>备注</th><th>时间</th><th>USDT</th><th>操作人</th></tr></thead><tbody>`;
                        for (const bill of data.expense_bills) {
                            html += `<tr>
                                <td>${bill.remark || '-'}</td>
                                <td>${bill.time}</td>
                                <td>${bill.usdt}U</td>
                                <td>${bill.username}</td>
                            </tr>`;
                        }
                        html += `</tbody></table></div>`;
                    } else {
                        html += `<div class="section"><div class="section-title">📤 下发记录</div><div class="loading">暂无下发记录</div></div>`;
                    }
                    
                    if (data.remark_stats && data.remark_stats.length > 0) {
                        html += `<div class="section"><div class="section-title">📊 备注分类统计</div>`;
                        for (const stat of data.remark_stats) {
                            html += `<div class="stat-item"><span class="stat-name">📝 ${stat.remark}</span><span class="stat-number">${stat.count}笔 | ${stat.amount}元 | ${stat.usdt}U</span></div>`;
                        }
                        html += `</div>`;
                    }
                    
                    if (data.operator_stats && data.operator_stats.length > 0) {
                        html += `<div class="section"><div class="section-title">👤 操作人统计</div>`;
                        for (const stat of data.operator_stats) {
                            html += `<div class="stat-item"><span class="stat-name">👤 ${stat.username}</span><span class="stat-number">${stat.count}笔 | ${stat.amount}元 | ${stat.usdt}U</span></div>`;
                        }
                        html += `</div>`;
                    }
                    
                    html += `<div class="stats-box"><div class="stats-grid">
                        <div class="stat-card"><div class="stat-label">💰 费率</div><div class="stat-value">${data.fee_rate}<span class="stat-unit">%</span></div></div>
                        <div class="stat-card"><div class="stat-label">💱 汇率</div><div class="stat-value">${data.exchange_rate}</div></div>
                        <div class="stat-card"><div class="stat-label">📥 总入款(元)</div><div class="stat-value">${data.total_rmb}</div></div>
                        <div class="stat-card"><div class="stat-label">💵 总入款(USDT)</div><div class="stat-value">${data.total_usdt}${data.show_usdt ? 'U' : ''}</div></div>
                        <div class="stat-card"><div class="stat-label">📤 已下发</div><div class="stat-value">${data.expense_usdt}<span class="stat-unit">U</span></div></div>
                        <div class="stat-card"><div class="stat-label">📊 未下发</div><div class="stat-value">${data.remaining_usdt}${data.show_usdt ? 'U' : ''}</div></div>
                    </div></div>`;
                    
                    document.getElementById('content').innerHTML = html;
                } catch (err) {
                    document.getElementById('content').innerHTML = '<div class="loading">加载失败，请稍后重试</div>';
                }
            }
            function prevDay() { const d = new Date(currentDate); d.setDate(d.getDate() - 1); currentDate = d.toISOString().split('T')[0]; document.getElementById('datePicker').value = currentDate; loadData(); }
            function nextDay() { const d = new Date(currentDate); d.setDate(d.getDate() + 1); currentDate = d.toISOString().split('T')[0]; document.getElementById('datePicker').value = currentDate; loadData(); }
            if (getDateFromURL()) { loadData(); }
        </script>
    </body>
    </html>
    '''

@flask_app.route('/api/bill')
def api_bill():
    date_str = request.args.get('date')
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    group_id = request.args.get('group_id', type=int, default=0)
    bills, total_income, total_expense = get_bills_by_date(group_id, date_str)
    
    rate = get_setting(group_id, 'exchange_rate') or 7.2
    fee_rate = get_setting(group_id, 'fee_rate') or 0
    show_usdt = get_setting(group_id, 'show_usdt') or 1
    
    total_rmb = total_income[0] or 0
    total_usdt = total_income[1] or 0
    expense_usdt = total_expense[0] or 0
    
    income_bills = []
    expense_bills = []
    
    for bill in bills:
        remark, username, amount, usdt, ex_rate, bill_type, ts = bill
        time_str = ts[11:16] if len(ts) > 11 else ts
        if bill_type == 'income':
            income_bills.append({
                'remark': remark or '-',
                'username': username,
                'amount': f"{amount:.0f}",
                'usdt': f"{usdt:.2f}",
                'exchange_rate': f"{ex_rate:.2f}",
                'time': time_str,
                'show_usdt': show_usdt
            })
        else:
            expense_bills.append({
                'remark': remark or '-',
                'username': username,
                'usdt': f"{usdt:.2f}",
                'time': time_str
            })
    
    remark_stats = []
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT remark, COUNT(*), SUM(amount), SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'income' AND date(timestamp) = ? GROUP BY remark ORDER BY SUM(usdt_amount) DESC", 
              (group_id, date_str))
    for row in c.fetchall():
        remark_stats.append({
            'remark': row[0] if row[0] else '无备注',
            'count': row[1],
            'amount': f"{row[2]:.0f}",
            'usdt': f"{row[3]:.2f}"
        })
    
    operator_stats = []
    c.execute("SELECT username, COUNT(*), SUM(amount), SUM(usdt_amount) FROM bills WHERE group_id = ? AND bill_type = 'income' AND date(timestamp) = ? GROUP BY username ORDER BY SUM(usdt_amount) DESC", 
              (group_id, date_str))
    for row in c.fetchall():
        operator_stats.append({
            'username': row[0],
            'count': row[1],
            'amount': f"{row[2]:.0f}",
            'usdt': f"{row[3]:.2f}"
        })
    conn.close()
    
    return jsonify({
        'exchange_rate': f"{rate:.2f}",
        'fee_rate': f"{fee_rate:.0f}",
        'total_rmb': f"{total_rmb:.0f}",
        'total_usdt': f"{total_usdt:.2f}",
        'expense_usdt': f"{expense_usdt:.2f}",
        'remaining_usdt': f"{total_usdt - expense_usdt:.2f}",
        'show_usdt': show_usdt,
        'income_bills': income_bills,
        'expense_bills': expense_bills,
        'remark_stats': remark_stats,
        'operator_stats': operator_stats
    })

# ========== 机器人命令 ==========

def get_bill_content(income, expense, total_rmb, total_usdt, expense_usdt, rate, show_usdt, today_date, lang):
    """生成账单内容（支持多语言和显示/隐藏U）"""
    if lang == 'myanmar':
        income_title = "📥 ဝင်ငွေ"
        expense_title = "📤 ထုတ်ငွေ"
        no_data = "စာရင်းမရှိပါ"
        more_text = "နောက်ထပ်"
        exchange_text = "💰 ငွေလဲနှုန်း"
        total_income_text = "📊 စုစုပေါင်းဝင်ငွေ"
        total_expense_text = "📊 ထုတ်ပြီး"
        remaining_text = "📊 ကျန်ငွေ"
        unit = "U"
    else:
        income_title = "📥 入款"
        expense_title = "📤 下发"
        no_data = "暂无记录"
        more_text = "还有"
        exchange_text = "💰 汇率"
        total_income_text = "📊 总入款"
        total_expense_text = "📊 已下发"
        remaining_text = "📊 未下发"
        unit = "U"
    
    message = f"📊 今日账单汇总 {today_date}\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    if income:
        message += f"{income_title}({len(income)} 笔):\n"
        for bill in income[:5]:
            remark, username, amount, usdt, ex_rate, ts = bill
            time_short = ts[11:16] if len(ts) > 11 else ts
            if remark:
                if show_usdt:
                    message += f"  {username}【{remark}】{time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f} {unit}\n"
                else:
                    message += f"  {username}【{remark}】{time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f}\n"
            else:
                if show_usdt:
                    message += f"  {username} {time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f} {unit}\n"
                else:
                    message += f"  {username} {time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f}\n"
        if len(income) > 5:
            message += f"  ... {more_text} {len(income)-5} 笔\n"
        message += "\n"
    else:
        message += f"{income_title}(0 笔):\n\n"
    
    if expense:
        message += f"{expense_title}({len(expense)} 笔):\n"
        for bill in expense[:5]:
            remark, username, usdt, ex_rate, ts = bill
            time_short = ts[11:16] if len(ts) > 11 else ts
            message += f"  {username} {time_short}  {usdt:.2f} {unit}\n"
        if len(expense) > 5:
            message += f"  ... {more_text} {len(expense)-5} 笔\n"
        message += "\n"
    else:
        message += f"{expense_title}(0 笔):\n\n"
    
    message += f"{exchange_text}：{rate:.2f}\n"
    if show_usdt:
        message += f"{total_income_text}：{total_rmb:.0f} | {total_usdt:.2f} {unit}\n"
        message += f"{total_expense_text}：{expense_usdt:.2f} {unit}\n"
        message += f"{remaining_text}：{total_usdt - expense_usdt:.2f} {unit}"
    else:
        message += f"{total_income_text}：{total_rmb:.0f} | {total_usdt:.2f}\n"
        message += f"{total_expense_text}：{expense_usdt:.2f} {unit}\n"
        message += f"{remaining_text}：{total_usdt - expense_usdt:.2f}"
    
    return message

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    rate = get_setting(gid, 'exchange_rate') or 7.2
    is_active = get_setting(gid, 'is_active') or 0
    status = "🟢 开启" if is_active else "🔴 关闭"
    message = f"🤖 *记账机器人已启动*\n\n📌 状态: {status}\n💰 汇率: 1 USDT = {rate:.2f} 元\n\n发送 /help 查看帮助"
    await update.message.reply_text(message, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    lang = get_setting(gid, 'language') or 'chinese'
    
    if lang == 'myanmar':
        help_text = """
📖 *ငွေစာရင်းဘော့အကူအညီ*

📌 *ငွေစာရင်းသွင်းနည်း：*
`+1000` - ၁၀၀၀ ကျပ်သွင်းရန်
`အမည်+2000` - မှတ်ချက်ထည့်သွင်းရန်
`ထုတ်50` - USDT 50 ထုတ်ရန်
`+0` - ယနေ့အကျဉ်းချုပ်ကြည့်ရန်
`/bill` - ဝဘ်လင့်ခ်ရယူရန်

📌 *စီမံခန့်ခွဲမှု：*
`/mode` - မုဒ်ဖွင့်/ပိတ်
`/setrate 7.2` - ငွေလဲနှုန်းသတ်မှတ်
`/setoperator` - အသုံးပြုသူသတ်မှတ်
`/listops` - အသုံးပြုသူစာရင်း
`/language` - ဘာသာစကားပြောင်း
`/timezone` - အချိန်ဇုန်ပြောင်း
`/showusdt` - USDT ပြရန်
`/hideusdt` - USDT ဝှက်ရန်

📌 *ဖျက်ခြင်း：*
`/deltoday` - ယနေ့စာရင်းဖျက်
`/dellast` - နောက်ဆုံးတစ်ခုဖျက်
`/delall` - အားလုံးဖျက်
"""
    else:
        help_text = """
🤖 *记账机器人帮助*

📌 *记账格式：*
`+1000` - 入款1000元
`အမည်+2000` - 带备注入款
`下发50` - 下发50 USDT
`+0` - 查看今日汇总
`/bill` - 获取网页账单链接

📌 *管理命令：*
`/mode` - 开启/关闭记账模式
`/setrate 7.2` - 设置汇率
`/setoperator` - 设置操作人（回复某人消息后发送）
`/listops` - 查看操作人列表
`/language` - 切换语言（中文/缅甸语）
`/timezone` - 设置时区
`/showusdt` - 显示USDT单位
`/hideusdt` - 隐藏USDT单位

📌 *删除命令：*
`/deltoday` - 删除今日所有账单
`/dellast` - 删除最后一笔账单
`/delall` - 删除所有账单
`/deluser 名字` - 删除某人的账单

📌 *历史查询：*
`/history 2026-05-13` - 查询指定日期账单
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    current = get_setting(gid, 'is_active') or 0
    if current == 0:
        update_setting(gid, 'is_active', 1)
        await update.message.reply_text("✅ 记账模式已开启\n\n现在可以发送记账命令了！")
    else:
        update_setting(gid, 'is_active', 0)
        await update.message.reply_text("🔕 记账模式已关闭")

async def setrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    if not context.args:
        await update.message.reply_text("📌 用法: /setrate 7.2")
        return
    try:
        rate = float(context.args[0])
        update_setting(gid, 'exchange_rate', rate)
        await update.message.reply_text(f"✅ 汇率已设为 {rate}")
    except:
        await update.message.reply_text("❌ 请输入正确的数字")

async def bill_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    today = datetime.now().strftime("%Y-%m-%d")
    web_url = f"{WEB_URL}?date={today}&group_id={gid}"
    await update.message.reply_text(
        f"📊 *查看完整账单*\n\n点击链接在网页中查看（可切换日期）：\n{web_url}\n\n"
        f"💡 提示：在网页里可以选择任意日期查看账单，还有分类统计",
        parse_mode='Markdown',
        disable_web_page_preview=False
    )

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    rate = get_setting(gid, 'exchange_rate') or 7.2
    is_active = get_setting(gid, 'is_active') or 0
    lang = get_setting(gid, 'language') or 'chinese'
    tz_str = get_setting(gid, 'timezone') or 'Asia/Shanghai'
    show_usdt = get_setting(gid, 'show_usdt') or 1
    ops = json.loads(get_setting(gid, 'operators') or '[]')
    
    status = "开启" if is_active else "关闭"
    timezone_name = "中国" if tz_str == 'Asia/Shanghai' else "缅甸" if tz_str == 'Asia/Yangon' else "泰国"
    language_name = "中文" if lang == 'chinese' else "缅甸语"
    usdt_status = "显示" if show_usdt else "隐藏"
    
    message = f"⚙️ *当前设置*\n"
    message += f"💰 汇率: {rate}\n"
    message += f"🔘 模式: {status}\n"
    message += f"🌍 时区: {timezone_name}\n"
    message += f"📖 语言: {language_name}\n"
    message += f"💵 USDT显示: {usdt_status}\n"
    message += f"👤 操作人: {len(ops)}人"
    await update.message.reply_text(message, parse_mode='Markdown')

async def setoperator_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    gid = update.effective_chat.id
    if not is_master(uid):
        await update.message.reply_text("❌ 只有机器人主人可以设置操作人")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text("❌ 请回复要设置为操作人的消息")
        return
    target = update.message.reply_to_message.from_user
    ops = json.loads(get_setting(gid, 'operators') or '[]')
    if target.id not in ops:
        ops.append(target.id)
        update_setting(gid, 'operators', json.dumps(ops))
        await update.message.reply_text(f"✅ 已设置 {target.first_name} 为操作人")
    else:
        await update.message.reply_text("该用户已经是操作人")

async def listops_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    ops = json.loads(get_setting(gid, 'operators') or '[]')
    if not ops:
        await update.message.reply_text("📋 暂无操作人")
        return
    message = "📋 操作人列表:\n"
    for oid in ops:
        try:
            member = await context.bot.get_chat_member(gid, oid)
            message += f"  • {member.user.first_name}\n"
        except:
            message += f"  • ID: {oid}\n"
    await update.message.reply_text(message)

async def language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    current = get_setting(gid, 'language') or 'chinese'
    if current == 'chinese':
        new_lang = 'myanmar'
        await update.message.reply_text("✅ 已切换为缅甸语\n✅ မြန်မာဘာသာသို့ ပြောင်းပြီး")
    else:
        new_lang = 'chinese'
        await update.message.reply_text("✅ 已切换为中文")
    update_setting(gid, 'language', new_lang)

async def timezone_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    if not context.args:
        tz_list = "📌 可用时区:\n  /timezone china - 中国北京时间\n  /timezone myanmar - 缅甸\n  /timezone thailand - 泰国"
        await update.message.reply_text(tz_list)
        return
    tz_name = context.args[0].lower()
    if tz_name in TIMEZONES:
        update_setting(gid, 'timezone', TIMEZONES[tz_name])
        await update.message.reply_text(f"✅ 时区已切换")
    else:
        await update.message.reply_text("❌ 无效的时区\n可用: china, myanmar, thailand")

async def show_usdt_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    update_setting(gid, 'show_usdt', 1)
    await update.message.reply_text("✅ 已开启USDT显示模式\n\n账单将同时显示人民币和USDT金额")

async def hide_usdt_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    update_setting(gid, 'show_usdt', 0)
    await update.message.reply_text("🔕 已关闭USDT显示模式\n\n账单将只显示人民币金额")

async def del_today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    deleted = delete_today_bills(gid)
    if deleted > 0:
        await update.message.reply_text(f"✅ 已删除今日所有账单，共 {deleted} 条记录")
    else:
        await update.message.reply_text("📭 今日暂无账单可删除")

async def del_last_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    deleted = delete_last_bill(gid)
    if deleted > 0:
        await update.message.reply_text("✅ 已删除最后一笔账单")
    else:
        await update.message.reply_text("📭 暂无账单可删除")

async def del_all_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    deleted = delete_all_bills(gid)
    if deleted > 0:
        await update.message.reply_text(f"✅ 已删除所有账单，共 {deleted} 条记录")
    else:
        await update.message.reply_text("📭 暂无账单可删除")

async def del_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    if not context.args:
        await update.message.reply_text("📌 用法: /deluser 名字")
        return
    target_name = ' '.join(context.args)
    deleted = delete_user_bills(gid, target_name)
    await update.message.reply_text(f"✅ 已删除 {target_name} 的账单，共 {deleted} 条记录")

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gid = update.effective_chat.id
    uid = update.effective_user.id
    if not can_use(gid, uid):
        await update.message.reply_text("❌ 你没有操作权限")
        return
    if not context.args:
        await update.message.reply_text("📅 用法: /history 2026-05-13\n\n例如: /history 2026-05-13")
        return
    date_str = context.args[0]
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except:
        await update.message.reply_text("❌ 日期格式错误！\n正确格式: 2026-05-13")
        return
    bills, total_income, total_expense = get_bills_by_date(gid, date_str)
    rate = get_setting(gid, 'exchange_rate') or 7.2
    show_usdt = get_setting(gid, 'show_usdt') or 1
    total_rmb = total_income[0] or 0
    total_usdt = total_income[1] or 0
    expense_usdt = total_expense[0] or 0
    if not bills:
        await update.message.reply_text(f"📭 {date_str} 没有账单记录")
        return
    message = f"📊 *历史账单*\n📅 {date_str}\n━━━━━━━━━━━━━━━━━━━━\n\n"
    income_bills = [b for b in bills if b[5] == 'income']
    if income_bills:
        message += f"📥 入款({len(income_bills)} 笔):\n"
        for bill in income_bills[:10]:
            remark, username, amount, usdt, ex_rate, _, ts = bill
            time_short = ts[11:16] if len(ts) > 11 else ts
            if remark:
                if show_usdt:
                    message += f"  {username}【{remark}】{time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f} U\n"
                else:
                    message += f"  {username}【{remark}】{time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f}\n"
            else:
                if show_usdt:
                    message += f"  {username} {time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f} U\n"
                else:
                    message += f"  {username} {time_short}  {amount:.0f} / {ex_rate:.0f} = {usdt:.2f}\n"
        message += "\n"
    message += f"💰 汇率：{rate:.2f}\n"
    if show_usdt:
        message += f"📊 总入款：{total_rmb:.0f} | {total_usdt:.2f} U\n📊 已下发：{expense_usdt:.2f} U\n📊 未下发：{total_usdt - expense_usdt:.2f} U"
    else:
        message += f"📊 总入款：{total_rmb:.0f} | {total_usdt:.2f}\n📊 已下发：{expense_usdt:.2f} U\n📊 未下发：{total_usdt - expense_usdt:.2f}"
    await update.message.reply_text(message, parse_mode='Markdown')

async def show_full_bill(update: Update, gid):
    income, expense, total_income, total_expense, today_date = get_today_bills(gid)
    rate = get_setting(gid, 'exchange_rate') or 7.2
    show_usdt = get_setting(gid, 'show_usdt') or 1
    lang = get_setting(gid, 'language') or 'chinese'
    
    total_rmb = total_income[0] or 0
    total_usdt = total_income[1] or 0
    expense_usdt = total_expense[0] or 0
    
    message = get_bill_content(income, expense, total_rmb, total_usdt, expense_usdt, rate, show_usdt, today_date, lang)
    
    keyboard = [[
        InlineKeyboardButton("📊 查看完整账单", url=f"{WEB_URL}?group_id={gid}"),
        InlineKeyboardButton("📖 帮助", callback_data='show_help')
    ]]
    await update.message.reply_text(message, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_today_summary(update: Update, gid):
    income, expense, total_income, total_expense, today_date = get_today_bills(gid)
    rate = get_setting(gid, 'exchange_rate') or 7.2
    show_usdt = get_setting(gid, 'show_usdt') or 1
    lang = get_setting(gid, 'language') or 'chinese'
    
    total_rmb = total_income[0] or 0
    total_usdt = total_income[1] or 0
    expense_usdt = total_expense[0] or 0
    
    message = get_bill_content(income, expense, total_rmb, total_usdt, expense_usdt, rate, show_usdt, today_date, lang)
    
    keyboard = [[
        InlineKeyboardButton("📊 查看完整账单", url=f"{WEB_URL}?group_id={gid}"),
        InlineKeyboardButton("📖 帮助", callback_data='show_help')
    ]]
    await update.message.reply_text(message, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def show_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    gid = update.effective_chat.id
    lang = get_setting(gid, 'language') or 'chinese'
    
    if lang == 'myanmar':
        help_text = """
📖 *ငွေစာရင်းဘော့အကူအညီ*

📌 *ငွေစာရင်းသွင်းနည်း：*
`+1000` - ၁၀၀၀ ကျပ်သွင်းရန်
`အမည်+2000` - မှတ်ချက်ထည့်သွင်းရန်
`ထုတ်50` - USDT 50 ထုတ်ရန်
`+0` - ယနေ့အကျဉ်းချုပ်ကြည့်ရန်
`/bill` - ဝဘ်လင့်ခ်ရယူရန်

📌 *စီမံခန့်ခွဲမှု：*
`/mode` - မုဒ်ဖွင့်/ပိတ်
`/setrate 7.2` - ငွေလဲနှုန်းသတ်မှတ်
`/setoperator` - အသုံးပြုသူသတ်မှတ်
`/listops` - အသုံးပြုသူစာရင်း
`/language` - ဘာသာစကားပြောင်း
`/timezone` - အချိန်ဇုန်ပြောင်း
`/showusdt` - USDT ပြရန်
`/hideusdt` - USDT ဝှက်ရန်

📌 *ဖျက်ခြင်း：*
`/deltoday` - ယနေ့စာရင်းဖျက်
`/dellast` - နောက်ဆုံးတစ်ခုဖျက်
`/delall` - အားလုံးဖျက်
"""
    else:
        help_text = """
📖 *记账机器人帮助*

📌 *记账格式：*
`+1000` - 入款1000元
`အမည်+2000` - 带备注入款
`下发50` - 下发50 USDT
`+0` - 查看今日汇总
`/bill` - 获取网页账单链接

📌 *管理命令：*
`/mode` - 开启/关闭记账模式
`/setrate 7.2` - 设置汇率
`/setoperator` - 设置操作人
`/listops` - 查看操作人列表
`/language` - 切换语言
`/timezone` - 设置时区
`/showusdt` - 显示USDT单位
`/hideusdt` - 隐藏USDT单位

📌 *删除命令：*
`/deltoday` - 删除今日所有账单
`/dellast` - 删除最后一笔账单
`/delall` - 删除所有账单
`/deluser 名字` - 删除某人的账单

📌 *历史查询：*
`/history 2026-05-13` - 查询指定日期账单
"""
    keyboard = [[InlineKeyboardButton("🔙 返回", callback_data='back_to_main')]]
    await query.edit_message_text(help_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    gid = update.effective_chat.id
    rate = get_setting(gid, 'exchange_rate') or 7.2
    is_active = get_setting(gid, 'is_active') or 0
    status = "🟢 开启" if is_active else "🔴 关闭"
    message = f"🤖 *记账机器人*\n\n📌 状态: {status}\n💰 汇率: 1 USDT = {rate:.2f} 元\n"
    message += "━━━━━━━━━━━━━━━━━━━━\n\n📝 *记账格式:*\n`+1000` - 入款1000元\n"
    message += "`အမည်+2000` - 带备注入款\n`下发50` - 下发50 USDT\n`+0` - 查看今日汇总\n\n"
    message += "📌 *管理命令:*\n`/mode` - 开关记账模式\n`/setrate` - 设置汇率\n`/setoperator` - 设置操作人\n"
    message += "`/bill` - 查看今日账单\n`/language` - 切换语言\n`/timezone` - 设置时区\n"
    message += "`/deltoday` - 删除今日账单\n`/dellast` - 删除最后一笔\n`/delall` - 删除所有账单\n"
    message += "`/showusdt` - 显示USDT单位\n`/hideusdt` - 隐藏USDT单位"
    
    keyboard = [[
        InlineKeyboardButton("📊 查看完整账单", url=f"{WEB_URL}?group_id={gid}"),
        InlineKeyboardButton("📖 帮助", callback_data='show_help')
    ]]
    await query.edit_message_text(message, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def accounting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    gid = update.effective_chat.id
    uid = update.effective_user.id
    username = update.effective_user.first_name
    is_active = get_setting(gid, 'is_active') or 0
    if is_active == 0:
        return
    if not can_use(gid, uid):
        return
    if text == '+0':
        await show_today_summary(update, gid)
        return
    m = re.match(r'^下发(\d+(?:\.\d+)?)$', text)
    if m:
        amount = float(m.group(1))
        add_bill(gid, uid, username, '', amount, 'expense')
        await show_full_bill(update, gid)
        return
    m = re.match(r'^([^+\d]+)?\+(\d+(?:\.\d+)?)(?:/(\d+(?:\.\d+)?))?$', text)
    if m:
        remark = m.group(1).strip() if m.group(1) else ''
        amount = float(m.group(2))
        custom_rate = float(m.group(3)) if m.group(3) else None
        exchange_rate = custom_rate if custom_rate else get_setting(gid, 'exchange_rate') or 7.2
        add_bill(gid, uid, username, remark, amount, 'income', exchange_rate)
        await show_full_bill(update, gid)
        return

def run_web():
    flask_app.run(host='0.0.0.0', port=PORT)

def main():
    init_db()
    print("🤖 机器人启动中...")
    print(f"🌐 Web 服务启动在端口 {PORT}...")
    web_thread = threading.Thread(target=run_web, daemon=True)
    web_thread.start()
    
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("mode", mode_command))
    app.add_handler(CommandHandler("setrate", setrate_command))
    app.add_handler(CommandHandler("bill", bill_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("setoperator", setoperator_command))
    app.add_handler(CommandHandler("listops", listops_command))
    app.add_handler(CommandHandler("language", language_command))
    app.add_handler(CommandHandler("timezone", timezone_command))
    app.add_handler(CommandHandler("showusdt", show_usdt_command))
    app.add_handler(CommandHandler("hideusdt", hide_usdt_command))
    app.add_handler(CommandHandler("deltoday", del_today_command))
    app.add_handler(CommandHandler("dellast", del_last_command))
    app.add_handler(CommandHandler("delall", del_all_command))
    app.add_handler(CommandHandler("deluser", del_user_command))
    app.add_handler(CallbackQueryHandler(show_help, pattern='show_help'))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern='back_to_main'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, accounting))
    
    print("✅ 机器人运行中...")
    print(f"📊 网页访问地址: {WEB_URL}")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
