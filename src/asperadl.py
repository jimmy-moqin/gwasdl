import pandas as pd
import os
import subprocess as sp
import multiprocessing as mp


def construct_dl_commands(row: pd.DataFrame,
                          dir_base: str,
                          openssh: str) -> str:
    """生成下载命令，排除已存在文件"""
    url = row['url']
    dirs = os.path.join(dir_base, url.split('/')[-3], "harmonised/")
    

    # 定义 ascp 命令模板
    runstr = "ascp -QT -k 3 -l 300m -P 33001 -i {openssh} -T fasp-ebi@fasp.ebi.ac.uk:{filepath} {dirpath}"
    commands = []

    if (pd.notna(row["Hfile"])) and (pd.notna(row["yamlfile"])):
        hfile_path = os.path.join(dirs, row["Hfile"])
        yamlfile_path = os.path.join(dirs, row["yamlfile"])
        hfile_url = "/".join(url.split('/')[4:-1]) + "/" + row["Hfile"]
        yaml_url = "/".join(url.split('/')[4:-1]) + "/" + row["yamlfile"]

        if not os.path.exists(hfile_path):
            commands.append(runstr.format(openssh=openssh,
                            filepath=hfile_url, dirpath=dirs))
            os.makedirs(dirs, exist_ok=True)
        else:    
            print("File already exists:", hfile_path)
        if not os.path.exists(yamlfile_path):
            commands.append(runstr.format(openssh=openssh,
                            filepath=yaml_url, dirpath=dirs))
            os.makedirs(dirs, exist_ok=True)
        else:
            print("File already exists:", yamlfile_path)
    else:
        ffile_name, ffile_path, ffile_url = None, None, None
        if pd.notna(row["Ffile37"]):
            ffile_name = row["Ffile37"]
        elif pd.notna(row["Ffile38"]):
            ffile_name = row["Ffile38"]
        else:
            pass

        if ffile_name:
            ffile_path = os.path.join(dirs, ffile_name)
            ffile_url = "/".join(url.split('/')[4:-1]) + "/" + ffile_name
            if not os.path.exists(ffile_path):
                commands.append(runstr.format(openssh=openssh,
                                filepath=ffile_url, dirpath=dirs))
                os.makedirs(dirs, exist_ok=True)
            else:
                print("File already exists:", ffile_path)
        else:
            print("No valid file found for this URL:", url)

    return commands


def download_file(cmd:str):
    """执行下载命令"""
    sp.run(cmd, shell=True)


def download_gwas_files(dl_urls_file:str, 
                        dir_base:str, 
                        openssh:str, 
                        cmdfile:str,
                        process_count:int=10):
    """主函数，读取 URL，构建下载命令并执行下载"""
    # 读取并过滤文件
    dl_urls = pd.read_csv(dl_urls_file)
    dl_urls = dl_urls[dl_urls['isExist'] == 'yes']
    dl_list = []

    # 构建下载命令列表
    for _, row in dl_urls.iterrows():
        dl_list.extend(construct_dl_commands(row, dir_base, openssh))

    # 去除重复命令
    dl_list = list(set(dl_list))
    with open(cmdfile, "w") as f:
        f.write("\n".join(dl_list))

    # 多进程下载
    with mp.Pool(process_count) as pool:
        pool.map(download_file, dl_list)



