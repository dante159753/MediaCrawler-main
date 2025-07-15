import asyncio
import sys
from typing import Optional
import pandas as pd
import os
import glob
import json
import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk, filedialog
import threading
import datetime
import shutil

import re
import cmd_arg
import config
import db
import requests
from base.base_crawler import AbstractCrawler
from media_platform.bilibili import BilibiliCrawler
from media_platform.douyin import DouYinCrawler
from media_platform.kuaishou import KuaishouCrawler
from media_platform.tieba import TieBaCrawler
from media_platform.weibo import WeiboCrawler
from media_platform.xhs import XiaoHongShuCrawler
from media_platform.zhihu import ZhihuCrawler


import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CrawlerFactory:
    CRAWLERS = {
        "xhs": XiaoHongShuCrawler,
        "dy": DouYinCrawler,
        "ks": KuaishouCrawler,
        "bili": BilibiliCrawler,
        "wb": WeiboCrawler,
        "tieba": TieBaCrawler,
        "zhihu": ZhihuCrawler
    }

    @staticmethod
    def create_crawler(platform: str) -> AbstractCrawler:
        crawler_class = CrawlerFactory.CRAWLERS.get(platform)
        if not crawler_class:
            raise ValueError("Invalid Media Platform Currently only supported xhs or dy or ks or bili ...")
        return crawler_class()


def extract_links_from_text(text):
    """从文本中提取链接信息"""
    lines = text.strip().split('\n')
    links_data = []
    
    for line in lines:
        if not line.strip():
            continue
            
        # 提取用户名
        username_match = re.search(r'(\S+?)发布了', line)
        if not username_match:
            username_match = re.search(r'【([^】]+)', line)
        username = username_match.group(1) if username_match else "未知用户"
        
        # 提取链接
        urls = re.findall(r'http[s]?://[^\s]+', line)
        
        for url in urls:
            link_data = {
                'username': username,
                'url': url,
                'platform': detect_platform(url),
                'content': line.strip()
            }
            links_data.append(link_data)
    
    return links_data

def detect_platform(url):
    """检测链接平台"""
    if 'xiaohongshu.com' in url or 'xhslink.com' in url:
        return '小红书'
    elif 'douyin.com' in url:
        return '抖音'
    else:
        raise Exception(f"Unsupported platform for URL: {url}")


def save_to_excel(filename='social_media_stats.xlsx'):
    """保存结果到Excel文件，从JSON文件读取数据"""
    
    # 读取抖音JSON数据
    douyin_data = []
    douyin_json_path = "./data/douyin/json/"
    if os.path.exists(douyin_json_path):
        json_files = glob.glob(os.path.join(douyin_json_path, "*.json"))
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            douyin_data.append({
                                'username': item.get('nickname', '未知用户'),
                                'platform': '抖音',
                                'url': item.get('aweme_url', ''),
                                'likes': item.get('liked_count', '0'),
                                'comments': item.get('comment_count', '0'),
                                'collects': item.get('collected_count', '0'),
                                'shares': item.get('share_count', '0'),
                                'content': item.get('title', '')
                            })
            except Exception as e:
                logger.error(f"Failed to read {json_file}: {e}")
    
    # 读取小红书JSON数据
    xhs_data = []
    xhs_json_path = "./data/xhs/json/"
    if os.path.exists(xhs_json_path):
        json_files = glob.glob(os.path.join(xhs_json_path, "*.json"))
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            xhs_data.append({
                                'username': item.get('nickname', '未知用户'),
                                'platform': '小红书',
                                'url': item.get('note_url', ''),
                                'likes': item.get('liked_count', '0'),
                                'comments': item.get('comment_count', '0'),
                                'collects': item.get('collected_count', '0'),
                                'shares': item.get('share_count', '0'),
                                'content': item.get('title', '')
                            })
            except Exception as e:
                logger.error(f"Failed to read {json_file}: {e}")
    
    # 合并所有数据
    all_data = douyin_data + xhs_data
    
    if not all_data:
        logger.warning("No data found in JSON files")
        return
    
    df = pd.DataFrame(all_data)
    
    # 重新排列列的顺序
    columns_order = [
        'username', 'platform', 'url', 
        'likes', 'comments', 'collects', 'shares', 'content'
    ]
    df = df[columns_order]
    
    # 重命名列
    df.columns = [
        '用户名', '平台', '链接', 
        '点赞数', '评论数', '收藏数', '分享数', '原始内容'
    ]
    
    # 保存到Excel
    filepath = f"{filename}"
    df.to_excel(filepath, index=False, engine='openpyxl')
    
    logger.info(f"Results saved to {filepath}")
    logger.info(f"Total records: {len(df)}")
    
    # 打印统计信息
    print(f"\n=== 爬取完成 ===")
    print(f"总共处理: {len(df)} 条记录")
    print(f"小红书: {len(df[df['平台'] == '小红书'])} 条")
    print(f"抖音: {len(df[df['平台'] == '抖音'])} 条")
    print(f"结果已保存到: {filepath}")
    
    return filepath

def get_douyin_id(url: str) -> str:
    if "v.douyin.com" in url:
        response = requests.get(url, allow_redirects=False, timeout=10)
        if response.status_code in [301, 302, 307]:
            url = response.headers.get('Location', url)
            print(f"dy Redirected to: {url}")
    match = re.search(r'note/(\d+)', url)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Invalid Douyin URL: {url}. Unable to extract ID.")


async def crawl_data(input_text, save_path):
    """爬取数据的核心逻辑"""
    # Delete all JSON files in data directories
    douyin_json_path = "./data/douyin/json/"
    xhs_json_path = "./data/xhs/json/"
    
    for path in [douyin_json_path, xhs_json_path]:
        if os.path.exists(path):
            json_files = glob.glob(os.path.join(path, "*.json"))
            for json_file in json_files:
                try:
                    os.remove(json_file)
                    logger.info(f"Deleted: {json_file}")
                except Exception as e:
                    logger.error(f"Failed to delete {json_file}: {e}")
    
    config.CRAWLER_TYPE = "detail"

    links = extract_links_from_text(input_text)

    xhslinks = [link for link in links if link['platform'] == '小红书']
    dylinks = [link for link in links if link['platform'] == '抖音']
    config.XHS_SPECIFIED_NOTE_URL_LIST = [link['url'] for link in xhslinks]
    config.DY_SPECIFIED_ID_LIST = [get_douyin_id(link['url']) for link in dylinks]

    crawler = CrawlerFactory.create_crawler(platform="xhs")
    await crawler.start()
    
    crawler = CrawlerFactory.create_crawler(platform="dy")
    await crawler.start()
    
    # 爬取完成后，生成Excel文件
    return save_to_excel(save_path)


class CrawlerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("社交媒体数据爬虫工具")
        self.root.geometry("800x600")
        
        # 创建主框架
        main_frame = ttk.Frame(root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)
        
        # 标题
        title_label = ttk.Label(main_frame, text="社交媒体链接数据爬取工具", font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, pady=(0, 10), sticky=tk.W)
        
        # 输入框标签
        input_label = ttk.Label(main_frame, text="请输入包含小红书或抖音链接的文本，每行一条：")
        input_label.grid(row=1, column=0, sticky=tk.W, pady=(0, 5))
        
        # 输入框
        self.text_input = scrolledtext.ScrolledText(main_frame, height=20, width=80)
        self.text_input.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 设置默认文本
        default_text = """20 巧克力酱酱.发布了一篇小红书笔记，快来看吧！ 😆 rDVoUzA9mQWI5MJ 😆 http://xhslink.com/a/9xEPz6muLDofb，复制本条信息，打开【小红书】App查看精彩内容！
52 拾贰发布了一篇小红书笔记，快来看吧！ 😆 ZVxybzkZNa7lWEU 😆 http://xhslink.com/a/LBoH4igKHbofb，复制本条信息，打开【小红书】App查看精彩内容！
2.84 复制打开抖音，看看【雪时的图文作品】吉伊演唱会。# chiikawa吉伊 # chii... https://v.douyin.com/qAvG3Q0bJbc/ m@Q.XZ 07/15 icn:/
4.69 复制打开抖音，看看【风苒的图文作品】温迪生日快乐！不管是什么样的温迪都是一样的可爱呢 ... https://v.douyin.com/ckBTSnFL_1Y/ eOX:/ G@i.ca 05/05"""
        self.text_input.insert("1.0", default_text)
        
        # 按钮框架
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=3, column=0, pady=10)
        
        # 获取数据按钮
        self.crawl_button = ttk.Button(button_frame, text="获取数据", command=self.start_crawling)
        self.crawl_button.pack(side=tk.LEFT, padx=(0, 10))
        
        # 清除登录信息按钮
        self.clear_login_button = ttk.Button(button_frame, text="清除登录信息", command=self.clear_login_data)
        self.clear_login_button.pack(side=tk.LEFT, padx=(0, 10))
        
        # 输入框标签
        input_label = ttk.Label(main_frame, text="使用时会弹出小红书和抖音登录页面，需要登陆后才能获取数据。登录小红书会伪装成 Mac OS X 设备，请放心使用")
        input_label.grid(row=4, column=0, sticky=tk.W, pady=(0, 5))
        
        
        # 状态标签
        self.status_label = ttk.Label(main_frame, text="准备就绪", foreground="green")
        self.status_label.grid(row=5, column=0, sticky=tk.W, pady=(10, 0))
    
    def clear_login_data(self):
        """清除登录信息"""
        try:
            browser_data_path = "./browser_data"
            if os.path.exists(browser_data_path):
                # 删除browser_data目录下的所有内容
                for item in os.listdir(browser_data_path):
                    item_path = os.path.join(browser_data_path, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                
                self.status_label.config(text="登录信息已清除", foreground="green")
                messagebox.showinfo("成功", "登录信息已清除！下次使用时需要重新登录。")
                logger.info("Browser data cleared successfully")
            else:
                messagebox.showinfo("提示", "没有找到登录信息文件")
                
        except Exception as e:
            error_msg = f"清除登录信息失败: {str(e)}"
            self.status_label.config(text="清除失败", foreground="red")
            messagebox.showerror("错误", error_msg)
            logger.error(error_msg)
        
    def start_crawling(self):
        """开始爬取数据"""
        input_text = self.text_input.get("1.0", tk.END).strip()
        
        if not input_text:
            messagebox.showerror("错误", "请输入要爬取的文本内容！")
            return
        
        # 生成默认文件名
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"点评赞数据-{current_time}.xlsx"
        
        # 让用户选择保存路径
        save_path = filedialog.asksaveasfilename(
            title="选择保存位置",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile=default_filename
        )
        
        if not save_path:  # 用户取消了保存对话框
            return
        
        # 禁用按钮
        self.crawl_button.config(state="disabled")
        self.clear_login_button.config(state="disabled")
        self.status_label.config(text="正在爬取数据，请稍候...", foreground="blue")
        
        # 在新线程中运行爬虫
        def run_crawler():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(crawl_data(input_text, save_path))
                loop.close()
                
                # 在主线程中更新UI
                self.root.after(0, self.crawling_finished, result)
                
            except Exception as e:
                self.root.after(0, self.crawling_error, str(e))
        
        threading.Thread(target=run_crawler, daemon=True).start()
    
    def crawling_finished(self, result_file):
        """爬取完成后的处理"""
        self.crawl_button.config(state="normal")
        self.clear_login_button.config(state="normal")
        self.status_label.config(text=f"爬取完成！结果已保存到: {result_file}", foreground="green")
        messagebox.showinfo("完成", f"数据爬取完成！\n结果已保存到: {result_file}")
    
    def crawling_error(self, error_msg):
        """爬取出错时的处理"""
        self.crawl_button.config(state="normal")
        self.clear_login_button.config(state="normal")
        self.status_label.config(text="爬取失败", foreground="red")
        messagebox.showerror("错误", f"爬取过程中出现错误：\n{error_msg}")


def start_gui():
    """启动GUI应用"""
    root = tk.Tk()
    app = CrawlerGUI(root)
    root.mainloop()


if __name__ == '__main__':
    try:
        # 启动GUI而不是直接运行爬虫
        start_gui()
    except KeyboardInterrupt:
        sys.exit()
