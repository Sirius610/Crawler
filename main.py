import os
import time
import re
import yaml
import json
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ================= 配置加载逻辑 =================
def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ================= 飞书推送客户端 =================
class FeishuClient:
    def __init__(self, config):
        self.app_id = config.get('app_id')
        self.app_secret = config.get('app_secret')
        self.app_token = config.get('app_token')
        self.table_id = config.get('table_id')
        self.token = None

    def get_tenant_access_token(self):
        """获取飞书访问凭证"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        try:
            resp = requests.post(url, json=payload).json()
            if resp.get("code") == 0:
                self.token = resp.get("tenant_access_token")
                return True
            else:
                print(f"  [!] 飞书认证失败: {resp}")
                return False
        except Exception as e:
            print(f"  [!] 飞书请求异常: {e}")
            return False

    def add_records(self, records):
        """批量写入数据到多维表格"""
        if not self.token and not self.get_tenant_access_token():
            return

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        feishu_records = []
        for item in records:
            feishu_records.append({
                "fields": {
                    "发布日期": item.get("发布日期", ""),
                    "标题": item.get("标题", ""),
                    "项目类型": item.get("项目类型", ""),
                    "公告类型": item.get("公告类型", ""),
                    "详情页链接": {
                        "text": "点击查看详情",
                        "link": item.get("详情页链接", "")
                    },
                    "公告内容": item.get("公告内容", "")[:2000] # 截断防止超长报错
                }
            })

        payload = {"records": feishu_records}
        
        try:
            resp = requests.post(url, headers=headers, json=payload).json()
            if resp.get("code") == 0:
                print(f"  [√] 成功推送 {len(records)} 条数据到飞书！")
            else:
                print(f"  [!] 推送飞书失败: {resp.get('msg')}")
        except Exception as e:
            print(f"  [!] 推送网络异常: {e}")


# ================= 核心爬虫逻辑 =================
class AdvancedTenderScraper:
    def __init__(self, config):
        self.config = config
        self.final_data = []
        
        # 记录已抓取ID，防止重复抓取
        self.history_file = "history.txt"
        self.seen_ids = self.load_history()
        
        self.target_types = config['target_project_types']
        self.target_key = config['target_notice_key']
        self.blacklist = config['blacklist_titles']
        self.only_today = config['only_today']
        
        self.today_str = datetime.now().strftime("%Y%m%d")

        self.feishu = None
        if config.get('feishu', {}).get('enable'):
            self.feishu = FeishuClient(config['feishu'])

    def load_history(self):
        if os.path.exists(self.history_file):
            with open(self.history_file, "r", encoding="utf-8") as f:
                return set(f.read().splitlines())
        return set()

    def save_history(self):
        with open(self.history_file, "w", encoding="utf-8") as f:
            f.write("\n".join(self.seen_ids))

    def html_to_markdown(self, html_content):
        if not html_content:
            return ""

        soup = BeautifulSoup(html_content, 'html.parser')
        for br in soup.find_all("br"):
            br.replace_with("\n")

        table = soup.find('table')
        if not table:
            return soup.get_text(separator="\n", strip=True)

        markdown_lines = []
        rows = table.find_all('tr')

        for row in rows:
            cols = row.find_all(['td', 'th'])
            row_data = []
            for col in cols:
                text = col.get_text(separator=" ", strip=True)
                text = re.sub(r'[ \t]+', ' ', text).replace("\n", " ")
                row_data.append(text)
                try:
                    colspan = int(col.get("colspan", 1))
                    if colspan > 1:
                        for _ in range(colspan - 1):
                            row_data.append(" ")
                except:
                    pass
            if any(cell.strip() for cell in row_data):
                markdown_lines.append("| " + " | ".join(row_data) + " |")

        return "\n".join(markdown_lines)

    def extract_richtext_from_json(self, detail_json):
        try:
            column_list = detail_json.get("tradingNoticeColumnModelList", [])
            if column_list:
                for item in column_list:
                    if item.get("name") == "公告内容":
                        return item.get("richtext", "")
            return detail_json.get("noticeContent") or detail_json.get("richtext", "")
        except Exception as e:
            print(f"    [!] 提取 Richtext 失败: {e}")
            return ""

    def parse_api_targets(self, json_data):
        targets = []
        try:
            items = json_data.get("data", {}).get("pageData", [])
            for item in items:
                title = str(item.get("noticeTitle") or "")
                p_type = str(item.get("projectTypeName") or "")
                n_desc = str(item.get("noticeThirdTypeDesc") or "")
                pub_date = str(item.get("publishDate") or "")
                notice_id = str(item.get("noticeId") or "")

                # 提取纯数字日期，解决 '2026-03-05 10:30' 格式不匹配问题
                date_short = re.sub(r'\D', '', pub_date)[:8] if pub_date else ""

                # 【透视眼日志】打印每一条抓取到的原始数据，方便排查过滤原因
                print(f"  [透视] ID:{notice_id} | 日期:{date_short} | 类型:{p_type} | 公告:{n_desc} | 标题:{title[:15]}...")

                if notice_id in self.seen_ids: continue
                if self.target_key not in n_desc: continue
                if not any(k in p_type for k in self.target_types): continue
                if any(bad in title for bad in self.blacklist): continue
                
                # 日期判断
                if self.only_today and date_short != self.today_str: 
                    continue

                self.seen_ids.add(notice_id)
                print(f"  [+] 成功命中: {date_short} | {title[:15]}...")

                targets.append({
                    "title": title,
                    "base_info": {
                        "发布日期": date_short,
                        "标题": title,
                        "项目类型": p_type,
                        "公告类型": n_desc
                    }
                })
        except Exception as e:
            print(f"    [!] 列表解析异常: {e}")
        return targets

    def process_details(self, context, page, targets):
        if not targets:
            return

        for target in targets:
            title = target["title"]
            base_info = target["base_info"]
            search_text = title[:15] if len(title) > 15 else title

            try:
                with context.expect_page() as new_page_info:
                    try:
                        page.locator(f"text={search_text}").first.click(timeout=5000)
                    except:
                        print(f"    [!] 无法点击链接: {search_text}")
                        continue
                        
                new_page = new_page_info.value
                detail_json = {}

                def on_response(resp):
                    if "trading-notice/new/detail" in resp.url and resp.status == 200:
                        try:
                            res = resp.json()
                            if res.get("code") == 200 or res.get("errcode") == 0:
                                nonlocal detail_json
                                detail_json = res.get("data", {})
                        except:
                            pass

                new_page.on("response", on_response)

                try:
                    new_page.wait_for_load_state("networkidle", timeout=6000)
                except:
                    pass

                if detail_json:
                    raw_html = self.extract_richtext_from_json(detail_json)
                    markdown_content = self.html_to_markdown(raw_html)
                    entry = {
                        **base_info,
                        "详情页链接": new_page.url,
                        "公告内容": markdown_content[:30000]
                    }
                    self.final_data.append(entry)
                    print(f"    [√] 获取成功: {len(markdown_content)} 字符")
                else:
                    print(f"    [x] 详情API拦截失败")

                new_page.close()
                time.sleep(1) 

            except Exception as e:
                print(f"    [!] 详情页操作失败: {e}")
                try: new_page.close() 
                except: pass

    def run(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True, 
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            # 强制 1080P 大屏，防止元素重叠遮挡
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()

            print(f"1. 访问主页: {self.config['base_url']}")
            page.goto(self.config['base_url'])
            
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except:
                print("页面加载超时，尝试继续...")

            try:
                print("2. 切换分类...")
                with page.expect_response(lambda r: "search/v2/items" in r.url, timeout=10000) as r_info:
                    page.get_by_text("房屋建筑和市政基础设施工程").first.click()

                targets = self.parse_api_targets(r_info.value.json())
                self.process_details(context, page, targets)

            except Exception as e:
                print(f"初始化/切换分类失败: {e}")
                browser.close()
                return

            # ================= 翻页循环 =================
            for i in range(self.config['max_pages'] - 1):
                print(f"\n--- 翻页中 (第 {i + 2} 页) ---")
                try:
                    # 还原为你本地运行正常的 icon-right 定位器
                    next_btn = page.locator("button:has(.icon-right)").first
                    try:
                        next_btn.wait_for(state="attached", timeout=5000)
                    except:
                        print("  [-] 找不到下一页按钮，可能网页未完全加载或已到底。")
                        break

                    if next_btn.is_disabled():
                        print("  [-] 下一页按钮被禁用，已到达最后一页。")
                        break

                    next_btn.scroll_into_view_if_needed()
                    time.sleep(1) 

                    # 延长等待并使用 JS 强制点击突破遮挡限制
                    with page.expect_response(lambda r: "search/v2/items" in r.url, timeout=20000) as r_info:
                        next_btn.evaluate("node => node.click()")

                    targets = self.parse_api_targets(r_info.value.json())
                    
                    if not targets:
                        print("  [-] 本页没有符合条件的新数据，继续检查下一页。")
                        continue
                        
                    self.process_details(context, page, targets)

                except Exception as e:
                    print(f"  [!] 翻页中断报错: {e}")
                    try:
                        page.screenshot(path=f"error_page_{i+2}.png")
                        print(f"  [i] 已保存错误截图: error_page_{i+2}.png")
                    except:
                        pass
                    break

            print(f"\n任务结束，共抓取 {len(self.final_data)} 条数据。")
            browser.close()

    def save(self):
        if self.final_data:
            df = pd.DataFrame(self.final_data)
            date_str = datetime.now().strftime('%Y%m%d_%H%M')
            filename = f"{self.config['output_prefix']}_{date_str}.xlsx"
            
            print(f"正在保存到 {filename} ...")
            df.to_excel(filename, index=False)
            print("本地Excel保存成功！")
            
            if self.feishu:
                print("正在推送到飞书...")
                self.feishu.add_records(self.final_data)
                
            self.save_history()
        else:
            print("没有抓取到新数据，不生成文件。")

if __name__ == "__main__":
    conf = load_config()
    s = AdvancedTenderScraper(conf)
    s.run()
    s.save()
