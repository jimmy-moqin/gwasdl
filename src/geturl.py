import pandas as pd
import asyncio
import aiohttp
from lxml.etree import HTML
from typing import Literal, Dict
# 生成下载文件列表


def generate_download_file_list(summary_file_path: str) -> pd.DataFrame:
    df = pd.read_csv(summary_file_path)
    df = df[df["FULL SUMMARY STATISTICS"] == "yes"]
    url_list = df["SUMMARY STATS LOCATION"].tolist()
    harmonised_list = [url + "/harmonised/" for url in url_list]
    df_harmonised = pd.DataFrame(
        {"url": harmonised_list, "isExist": "Pending",
         "Hfile": "Pending", "yamlfile": "Pending",
         "Ffile37": "Pending", "Ffile38": "Pending"})
    return df_harmonised

# 异步检查URL状态


async def check_url_status(url: str,
                           session: aiohttp.ClientSession,
                           semaphore: asyncio.Semaphore,
                           failed_urls: list) -> Literal['yes', 'no', 'unknown', 'error']:
    async with semaphore:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return "yes"
                elif response.status == 404:
                    return "no"
                else:
                    return "unknown"
        except:
            # 将失败的URL记录下来
            failed_urls.append(url)
            return "error"

# 解析HTML并提取body中的表格部分


def parse_html_table(html_content: str) -> list | None:
    tree = HTML(html_content)
    table = tree.xpath("//body//table")

    if table:
        rows = table[0].xpath(".//tr")
        parsed_data = []
        for row in rows:
            cols = row.xpath(".//td")
            for col in cols:
                a_element = col.xpath(".//a")
                if a_element:
                    link_href = a_element[0].get("href")
                    parsed_data.append(link_href)
        return parsed_data
    return None

# 异步解析具有harmonised路径的链接


async def parse_harmonised_url(url: str,
                               session: aiohttp.ClientSession,
                               semaphore: asyncio.Semaphore) -> Dict[str, str]:
    async with semaphore:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    parsed_table = parse_html_table(content)
                    if parsed_table:
                        res = {"url": url, "Hfile": "", "yamlfile": "",
                               "Ffile37": "", "Ffile38": ""}
                        for item in parsed_table:
                            if item.endswith("h.tsv.gz"):
                                res["Hfile"] = item
                            elif item.endswith("yaml"):
                                res["yamlfile"] = item
                            elif item.endswith("38.f.tsv.gz"):
                                res["Ffile38"] = item
                            elif item.endswith("37.f.tsv.gz"):
                                res["Ffile37"] = item
                        return res
                    else:
                        print(f"No valid table found in {url}")
                else:
                    print(
                        f"Failed to retrieve {url}, status: {response.status}")
        except Exception as e:
            print(f"Error occurred while parsing {url}: {e}")

# 主异步函数，用于检查所有URL的状态，并在最后重试失败的URL


async def check_all_url_status(df_harmonised: pd.DataFrame,
                               max_concurrent_requests: int) -> None:
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    failed_urls = []  # 存储初次失败的URL
    async with aiohttp.ClientSession() as session:
        tasks = [check_url_status(url, session, semaphore, failed_urls)
                 for url in df_harmonised["url"]]
        results = await asyncio.gather(*tasks)

        # 更新df_harmonised的isExist列
        df_harmonised["isExist"] = results

        # 重试失败的URL
        retry_results = []
        if failed_urls:
            retry_tasks = [check_url_status(
                url, session, semaphore, []) for url in failed_urls]
            retry_results = await asyncio.gather(*retry_tasks)

            # 更新重试后的结果
            for url, result in zip(failed_urls, retry_results):
                df_harmonised.loc[df_harmonised["url"]
                                  == url, "isExist"] = result

# 修改主异步函数，收集解析结果并更新到DataFrame


async def process_harmonised_links(df_harmonised: pd.DataFrame,
                                   max_concurrent_requests: int) -> None:
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    async with aiohttp.ClientSession() as session:
        tasks = []
        harmonised_urls = df_harmonised[df_harmonised["isExist"] == "yes"]["url"].tolist(
        )

        for url in harmonised_urls:
            task = asyncio.ensure_future(
                parse_harmonised_url(url, session, semaphore))
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # 将解析结果添加到DataFrame中
        for result in results:
            if result:
                df_harmonised.loc[df_harmonised["url"] == result["url"], ["Hfile", "yamlfile", "Ffile37", "Ffile38"]] = \
                    result["Hfile"], result["yamlfile"], result["Ffile37"], result["Ffile38"]

        print("Updated DataFrame:", df_harmonised)

# 同步执行异步任务


def main(summary_file_path: str,
         output_file_path: str,
         max_concurrent_requests: int = 8):
    df_harmonised = generate_download_file_list(summary_file_path)

    # 第一步：检查URL的状态
    print("[INFO]: Checking URL status...")
    asyncio.run(check_all_url_status(
        df_harmonised, max_concurrent_requests * 4)) # 额外增加一些并发请求
    # df_harmonised.to_csv("checked_harmonised_list.csv", index=False)

    # 第二步：处理实际具有harmonised路径的链接
    # df_harmonised = pd.read_csv("checked_harmonised_list.csv")
    print("[INFO]: Getting harmonised links...")
    asyncio.run(process_harmonised_links(
        df_harmonised, max_concurrent_requests))
    df_harmonised.to_csv(output_file_path, index=False)
    h_count = len(df_harmonised[df_harmonised["isExist"] == "yes"])

    return h_count
