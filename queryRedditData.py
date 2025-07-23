import praw

from flask import Flask, jsonify
from flask_cors import CORS
import requests
import boto3
import os
import pymysql
import re
import time
from datetime import datetime
import random

# 打包方法：python控制台执行：pyinstaller --onefile queryRedditData.py
# 创建 requirements.txt：pip freeze > requirements.txt

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 定义全局变量 s3
awsAccessKeyId = None
awsSecretAccessKey = None

reddit_read_only = praw.Reddit(client_id="stQdwONPRvtsubHL7B-Xhw",  # your client id
                               client_secret="owEqYePMDTC_8hA9RfjSiBNiiHvf8w",  # your client secret
                               user_agent="For Pzsw")  # your user agent


# ==== 主逻辑 ====
# http://127.0.0.1:10000/api/tasks/queryRedditData
@app.route("/api/tasks/queryRedditData", methods=["GET"])
def queryRedditData():
    try:
        print(f"\n📅 开始抓取：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 建立pzsw数据库连接
        db_conn_pzsw = connect_topzsw_db()
        db_cursor_pzsw = db_conn_pzsw.cursor()

        # pzsw
        all_infos = []

        # 多类图片抓取
        for category in ["top", "new", "best", "controversial"]:
            posts = fetch_pzsw_posts_by_type(category, limit=20)
            infos = handle_posts(posts, category)
            all_infos.extend(infos)

        # 批量写入数据库
        if all_infos:
            insert_pzsw_multiple_info(db_cursor_pzsw, all_infos)
            db_conn_pzsw.commit()

        print("✅ pzsw所有数据处理完成")
        return jsonify({"status": "success", "message": "Reddit 数据抓取成功"})
    except Exception as e:
        print(f"❌ 整体处理失败：{e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'db_conn_pzsw' in locals():
            db_conn_pzsw.close()
            print(" pzsw数据库连接已关闭")


# http://127.0.0.1:10000/api/tasks/getRedditLists
@app.route("/api/tasks/getRedditLists", methods=["GET"])
def getRedditLists():
    try:
        
        # 建立pzsw数据库连接
        db_conn_pzsw = connect_topzsw_db()
        db_cursor_pzsw = db_conn_pzsw.cursor()
        # 查询数据
        db_cursor_pzsw.execute("SELECT * FROM RedditLists ORDER BY save_time DESC LIMIT 100")
        rows = db_cursor_pzsw.fetchall()
        
        return rows
    except Exception as e:
        print(f"❌ 整体处理失败：{e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'db_conn_ccyzb' in locals():
            db_conn_ccyzb.close()
            print("ccyzb数据库连接已关闭")


def fetch_pzsw_posts_by_type(category, limit=20):
    if category == "top":
        return reddit_read_only.front.top(time_filter="day", limit=limit)
    elif category == "new":
        return reddit_read_only.front.new(limit=limit)
    elif category == "best":
        return reddit_read_only.front.best(limit=limit)
    elif category == "controversial":
        return reddit_read_only.front.controversial(time_filter="day", limit=limit)


def handle_posts(posts, category_name):
    seen_urls = set()
    collected_info = []

    for post in posts:
        if is_image_url(post.url) and post.url not in seen_urls:
            seen_urls.add(post.url)
            print(f"🎯 准备处理：{post.url}")
            local_info = download_image(post.url, category_name, post.title, post.selftext)
            if local_info:  # 成功下载才返回信息
                collected_info.append(local_info)

    return collected_info


def download_image(image_url, category, title=None, desc=None):
    try:
        base_dir = os.path.expanduser(f"~/Downloads/{category}")
        os.makedirs(base_dir, exist_ok=True)

        filename = image_url.split("/")[-1].split("?")[0]
        filepath = os.path.join(base_dir, filename)

        if os.path.exists(filepath):
            print(f"🟡 已存在：{filename}")
            return None

        response = requests.get(image_url, timeout=10)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"✅ 下载成功：{filename}")
            # 上传
            s3_key = f"redditdata/{filename}"
            https_url = upload_to_s3(filepath, "hzmttapps", s3_key)
            os.remove(filepath)
            return (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                str(int(time.time() * 1000)),
                clean_title_for_db(title + '-' + desc),
                generate_fancy_username(),  # 使用更好听的用户名
                https_url,
                '',
                1
            )
        else:
            print(f"❌ 下载失败：{image_url}")
    except Exception as e:
        print(f"🚫 异常下载：{e}")
    return None


def upload_to_s3(local_path, bucket_name, object_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id=awsAccessKeyId,
        aws_secret_access_key=awsSecretAccessKey,
        region_name='ap-southeast-2'
    )
    s3.upload_file(local_path, bucket_name, object_name)
    https_url = f"https://d1jas57su6omst.cloudfront.net/{object_name}"
    print(f"🌐 上传成功，访问链接：{https_url}")
    return https_url


def connect_topzsw_db():
    return pymysql.connect(
        host='112.124.47.33',
        port=3306,
        user='mayintao',
        password='Mayt@123',
        database='pzsw_apps',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def insert_pzsw_multiple_info(cursor, infos):
    sql = """INSERT IGNORE INTO RedditLists 
             (save_time, object_id, title, user_name, native_image_url, user_ip, isCanRelease) 
             VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    cursor.executemany(sql, infos)
    print(f"✅ 成功插入pzsw {len(infos)} 条数据")

def clean_title_for_db(title):
    cleaned = re.sub(r"[^\w\s\u4e00-\u9fff。，、！？：“”‘’《》（）()\-.,!?\"']", '', title)
    return cleaned.strip()[:200]


# === 工具函数 ===
def is_image_url(url):
    return url.lower().endswith(('.jpg', '.jpeg', '.png'))


def generate_fancy_username():
    adjectives = ['charming', 'sunny', 'silent', 'crazy', 'lucky', 'happy', 'pretty', 'sweet', 'cool', 'magic']
    nouns = ['girl', 'boy', 'dreamer', 'cat', 'dog', 'star', 'angel', 'flower', 'sun', 'moon']
    extras = ['x', 'z', 'pro', '88', '99', str(random.randint(100, 999))]

    username = f"{random.choice(adjectives)}_{random.choice(nouns)}_{random.choice(extras)}"
    return username


def get_aws_key():
    global awsAccessKeyId, awsSecretAccessKey  # 告诉 Python 这里我们用的是全局变量

    url = "https://mbtsserver.onrender.com/api/getAWSKey"
    params = {"key": "registeruser1739935308mbts"}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # 如果返回 4xx 或 5xx 会抛出异常

        data = response.json()
        print("✅ 成功获取数据：", data)
        # 文件存储服务
        try:
            sts = boto3.client(
                'sts',
                aws_access_key_id=data["accessKeyId"],
                aws_secret_access_key=data["secretAccessKey"],
                region_name='ap-southeast-2'
            )
            identity = sts.get_caller_identity()
            print(f"✅ 已找到 AWS 身份: {identity}")
            awsAccessKeyId = data["accessKeyId"]
            awsSecretAccessKey = data["secretAccessKey"]
        except Exception as e:
            print(f"❌ AWS 身份验证失败: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print("❌ 请求失败：", e)
        return None


if __name__ == "__main__":
    # 调用
    get_aws_key()

    port = int(os.environ.get("PORT", 10000))  # Render 默认 10000
    app.run(host="0.0.0.0", port=port)
