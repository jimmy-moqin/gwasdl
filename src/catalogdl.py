import pandas as pd
import subprocess as sp
import os


def download_catalog_file(catalog_file_url: str,
                          catalog_file_path: str,
                          dl_header: dict) -> bool:
    """ 下载GWAS Catalog文件 """
    if os.path.exists(catalog_file_path):
        return True

    headers_list = []
    for k, v in dl_header.items():
        headers_list.append("-H")
        headers_list.append(f'"{k}:{v}"')
    run_list = (['axel', '-q', '-c', '-o',
                catalog_file_path, '-n', '16'] +
                headers_list +
                [catalog_file_url]
                )
    print("Command: ", " ".join(run_list))
    result = sp.run(" ".join(run_list), shell=True)

    return result.returncode == 0  # 返回下载是否成功


def parse_summary_file(catalog_file_path: str,
                       summary_file_path: str,
                       last_updated_time: str) -> int:
    """ 解析下载的GWAS Catalog文件，并筛选所需的总结文件 """
    # 检测文件分隔符
    with open(catalog_file_path, 'r') as f:
        first_line = f.readline()
        sep = '\t' if len(first_line.split('\t')) >= 3 else ',' if len(
            first_line.split(',')) >= 3 else None
        if sep is None:
            raise ValueError('Unsupported file format')

    # 读取文件
    df = pd.read_csv(catalog_file_path, sep=sep)
    col_names = df.columns.tolist()
    df[col_names[0]] = pd.to_datetime(df[col_names[0]])

    # 筛选上次更新之后的数据
    mask = (df[col_names[0]] > last_updated_time)
    filtered_df = df.loc[mask]
    filtered_df = filtered_df[filtered_df["FULL SUMMARY STATISTICS"] == "yes"]
    count = len(filtered_df)
    # 保存筛选结果
    filtered_df.to_csv(summary_file_path, index=False)

    return count

# def main(catalog_file_url, catalog_file_path, summary_file_path, last_update_time, dl_header):
#     """ 主函数，执行下载和解析操作 """
#     if os.path.exists(catalog_file_path):
#         if not os.path.exists(summary_file_path):
#             parse_summary_file(catalog_file_path, summary_file_path, last_update_time)
#     else:
#         if download_catalog_file(catalog_file_url, catalog_file_path, dl_header):
#             parse_summary_file(catalog_file_path, summary_file_path, last_update_time)
#         else:
#             raise ValueError('Failed to download catalog file')


# # 示例调用 main 函数
# if __name__ == '__main__':
#     # 替换为实际值
#     catalog_file_url = "https://example.com/path/to/catalog"
#     catalog_file_path = "path/to/save/catalog_file.csv"
#     summary_file_path = "path/to/save/summary_file.csv"
#     last_update_time = "2023-01-01"
#     dl_header = {
#         "User-Agent": "Mozilla/5.0",
#         "Accept": "application/json"
#     }

#     main(catalog_file_url, catalog_file_path, summary_file_path, last_update_time, dl_header)
