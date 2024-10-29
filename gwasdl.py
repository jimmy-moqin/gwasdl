from src import catalogdl
from src import asperadl
from src import geturl
import os
import yaml
import rich
import argparse


def main():
    parser = argparse.ArgumentParser(description='GWAS Data Downloader')
    parser.add_argument(
        '-c', '--config', help='Path to config file', required=True)
    args = parser.parse_args()
    with open(args.config, 'r') as f:
        try:
            config = yaml.safe_load(f)

            catalog_url = config['GWASCatalogFileDownloadURL']
            catalog_dl_header = config['DownloadHeader']
            catalog_file_path = config['CatalogFilePath']
            summary_file_path = config['SummaryFilePath']
            harmonised_file_path = config['HarmonisedFilePath']
            last_updated_time = config['LastUpdatedTime']
            concurrent_num = config['ConcurrentNum']
            base_dir = config['BaseDir']
            aspera_ssh = config['AsperaSSH']
            process_num = config['ProcessNum']
            command_file = config['CommandFile']

        except yaml.YAMLError as e:
            raise ("[YAMLError]: Please open a valid YAML file!")

    # 下载GWAS Catalog文件
    if not os.path.exists(catalog_file_path):
        ret_code = catalogdl.download_catalog_file(
            catalog_file_url=catalog_url,
            dl_header=catalog_dl_header,
            catalog_file_path=catalog_file_path,
        )
        if ret_code:
            print(f"[INFO]: GWAS Catalog file downloaded successfully!")
            # 如果下载成功，则解析Catalog文件，并生成Summary文件
            count: int = catalogdl.parse_summary_file(
                catalog_file_path=catalog_file_path,
                summary_file_path=summary_file_path,
                last_updated_time=last_updated_time
            )
            print("[INFO]: GWAS Summary file parsed successfully!\n"
                  "[INFO]: {} GWAS studies found in the Catalog file."
                  "[INFO]: Summary file has been saved to {}.".format(
                      count, summary_file_path)
                  )
        else:
            print(f"[ERROR]: GWAS Catalog file download failed!")
    else:
        if os.path.exists(summary_file_path):
            print(f"[INFO]: GWAS Summary file already exists!")
        else:
            # 如果下载成功，则解析Catalog文件，并生成Summary文件
            count: int = catalogdl.parse_summary_file(
                catalog_file_path=catalog_file_path,
                summary_file_path=summary_file_path,
                last_updated_time=last_updated_time
            )
            print("[INFO]: GWAS Summary file parsed successfully!\n"
                  "[INFO]: {} GWAS studies found in the Catalog file."
                  "[INFO]: Summary file has been saved to {}.".format(
                      count, summary_file_path)
                  )

    # 根据summary文件，获取所有harmonised 的 GWAS数据集的URL
    if os.path.exists(harmonised_file_path):
        print(f"[INFO]: GWAS Harmonised filelist already exists!")
    else:
        h_count: int = geturl.main(
            summary_file_path=summary_file_path,
            output_file_path=harmonised_file_path,
            max_concurrent_requests=concurrent_num,
        )
        print("[INFO]: {} harmonised GWAS datasets found in the Catalog file."
              "[INFO]: Harmonised filelist has been saved to {}.".format(
                  h_count, harmonised_file_path)
              )
        
    # 执行下载
    asperadl.download_gwas_files(
        dl_urls_file=harmonised_file_path,
        dir_base=base_dir,
        openssh=aspera_ssh,
        cmdfile=command_file,
        process_count=process_num,
    )


if __name__ == '__main__':
    main()