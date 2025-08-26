#!/usr/bin/env python3
"""
ERA5 Surface Level Automated Workflow
Automatically downloads ERA5 single level variables and organizes them into separate folders
"""

import os
import sys
import zipfile
import shutil
import logging
import cdsapi
import subprocess
import re
from pathlib import Path
from datetime import datetime
import calendar
import argparse


class ERA5SLAutomatedWorkflow:
    def __init__(self, base_dir=".", log_level=logging.INFO, start_year=None, start_month=None, end_year=None, end_month=None):
        self.base_dir = Path(base_dir)
        self.tp_dir = self.base_dir / "tp"
        self.sl_dir = self.base_dir / "sl"
        self.download_dir = self.base_dir / "downloads"
        self.start_year = start_year
        self.start_month = start_month
        self.end_year = end_year
        self.end_month = end_month
        self.setup_logging(log_level)
        
    def setup_logging(self, log_level):
        """设置日志配置"""
        # 创建日志目录
        log_dir = self.base_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # 生成日志文件名（包含时间戳）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"era5_sl_workflow_{timestamp}.log"
        
        # 配置日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 创建logger
        self.logger = logging.getLogger('ERA5SLWorkflow')
        self.logger.setLevel(log_level)
        
        # 清除现有的处理器
        self.logger.handlers.clear()
        
        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        
        # 添加处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"日志系统初始化完成，日志文件: {log_file}")
        self.logger.info(f"工作目录: {self.base_dir.absolute()}")
        
    def create_output_directories(self):
        """创建输出文件夹（如果不存在）"""
        self.logger.info("开始创建输出目录")
        try:
            for directory in [self.tp_dir, self.sl_dir, self.download_dir]:
                if not directory.exists():
                    directory.mkdir(parents=True, exist_ok=True)
                    self.logger.info(f"成功创建文件夹: {directory}")
                    print(f"创建文件夹: {directory}")
                else:
                    self.logger.info(f"文件夹已存在: {directory}")
                    print(f"文件夹已存在: {directory}")
            
            # 创建temp目录用于存放拆分后的文件
            temp_dir = self.base_dir / "temp"
            if not temp_dir.exists():
                temp_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"成功创建临时文件夹: {temp_dir}")
                print(f"创建临时文件夹: {temp_dir}")
            else:
                self.logger.info(f"临时文件夹已存在: {temp_dir}")
                print(f"临时文件夹已存在: {temp_dir}")
        except Exception as e:
            self.logger.error(f"创建输出目录失败: {e}")
            raise
    
    def download_era5_data(self, year, month):
        """下载ERA5地表变量数据"""
        self.logger.info(f"开始下载 {year} 年 {month:02d} 月的ERA5地表变量数据")
        
        # 生成文件名
        filename = f"era5_sl_{year}{month:02d}.zip"
        file_path = self.download_dir / filename
        
        self.logger.info(f"处理 {year} 年 {month:02d} 月的数据")
        print(f"处理 {year} 年 {month:02d} 月的数据")
        
        # 检查文件是否已经存在
        if file_path.exists():
            file_size = file_path.stat().st_size
            self.logger.info(f"文件已存在，跳过下载: {file_path}, 文件大小: {file_size / (1024*1024):.2f} MB")
            print(f"文件已存在，跳过下载: {file_path}")
            return file_path
        
        # 构建下载请求
        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": [
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "2m_temperature",
                "mean_sea_level_pressure",
                "total_precipitation"
            ],
            "year": [str(year)],
            "month": [f"{month:02d}"],
            "day": self._get_days_for_month(year, month),
            "time": [
                "00:00", "06:00", "12:00", "18:00"
            ],
            "data_format": "netcdf",
            "download_format": "unarchived"
        }

        self.logger.debug(f"下载请求参数: {request}")

        try:
            client = cdsapi.Client()
            
            self.logger.info(f"开始下载ERA5地表变量数据到 {file_path}")
            print(f"开始下载ERA5地表变量数据到 {file_path}")
            
            client.retrieve(dataset, request, str(file_path))
            
            # 检查文件是否成功下载
            if file_path.exists():
                file_size = file_path.stat().st_size
                self.logger.info(f"下载完成: {file_path}, 文件大小: {file_size / (1024*1024):.2f} MB")
                print(f"下载完成: {file_path}")
                return file_path
            else:
                raise FileNotFoundError(f"下载的文件不存在: {file_path}")
                
        except Exception as e:
            self.logger.error(f"下载ERA5地表变量数据失败: {e}")
            raise
    
    def extract_and_organize_data(self, zip_file_path, year=None, month=None):
        """解压并组织数据到tp和sl文件夹"""
        self.logger.info(f"开始解压并组织数据: {zip_file_path}")
        print(f"开始解压并组织数据: {zip_file_path}")
        
        temp_extract_dir = self.download_dir / "temp_extract"
        temp_extract_dir.mkdir(exist_ok=True)
        
        try:
            # 解压zip文件
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)
            
            self.logger.info("Zip文件解压成功")
            
            # 从zip文件路径提取年月信息
            if year is None or month is None:
                zip_name = zip_file_path.stem
                match = re.search(r"era5_sl_(\d{4})(\d{2})", zip_name)
                if match:
                    year = int(match.group(1))
                    month = int(match.group(2))
                else:
                    # 默认年月
                    year = 2018
                    month = 2
            
            # 处理解压后的文件
            processed_files = 0
            for file_path in temp_extract_dir.rglob("*.nc"):
                if "stepType-accum" in file_path.name:
                    # 总降水文件 - 使用WSL中的split_era5.sh脚本处理
                    self._split_nc_file_with_wsl(file_path, self.tp_dir, "tp", year, month)
                    self.logger.info(f"处理降水数据: {file_path}")
                    print(f"处理降水数据: {file_path}")
                    processed_files += 1
                
                elif "stepType-instant" in file_path.name:
                    # 其他地表变量文件 - 使用WSL中的split_era5.sh脚本处理
                    self._split_nc_file_with_wsl(file_path, self.sl_dir, "sl", year, month)
                    self.logger.info(f"处理地表变量数据: {file_path}")
                    print(f"处理地表变量数据: {file_path}")
                    processed_files += 1
                
                else:
                    self.logger.warning(f"未识别的文件模式: {file_path.name}")
                    print(f"警告: 未识别的文件模式: {file_path.name}")
            
            self.logger.info(f"数据处理完成，共处理 {processed_files} 个文件")
            print(f"数据处理完成，共处理 {processed_files} 个文件")
            
            # 清理
            shutil.rmtree(temp_extract_dir)
            os.remove(zip_file_path)
            self.logger.info("清理完成")
            print("清理完成")
            
        except Exception as e:
            self.logger.error(f"解压和组织数据失败: {e}")
            # 清理临时文件以防出错
            if temp_extract_dir.exists():
                shutil.rmtree(temp_extract_dir, ignore_errors=True)
            # 清理temp目录
            temp_dir = self.base_dir / "temp"
            if temp_dir.exists():
                for file_path in temp_dir.glob("era5_*.nc"):
                    try:
                        file_path.unlink()
                    except:
                        pass
            raise
    
    def _split_nc_file_with_wsl(self, nc_file_path, output_dir, data_type, year, month):
        """
        使用WSL中的split_era5_sl.sh脚本将nc文件按时间步拆分
        
        Args:
            nc_file_path (Path): 输入的nc文件路径
            output_dir (Path): 输出目录
            data_type (str): 数据类型 ("sl" 或 "tp")
            year (int): 年份
            month (int): 月份
        """
        self.logger.info(f"开始使用WSL拆分文件: {nc_file_path.name}")
        
        try:
            # 将nc文件复制到基础目录，以便WSL脚本可以访问
            temp_nc_file = self.base_dir / nc_file_path.name
            shutil.copy2(nc_file_path, temp_nc_file)
            
            # 构建WSL命令，设置环境变量传递年月信息
            env_vars = f"ERA5_YEAR={year} ERA5_MONTH={month:02d}"
            cmd = ["wsl", "-d", "Ubuntu-24.04", "-e", "bash", "-c", 
                   f"cd '{self.base_dir.as_posix()}' && {env_vars} ./split_era5_sl.sh {nc_file_path.name}"]
            
            self.logger.info(f"执行WSL命令: {' '.join(cmd[:-1])} \"{cmd[-1]}\"")
            print(f"在WSL中执行拆分脚本: split_era5_sl.sh {nc_file_path.name}")
            
            # 执行WSL命令，添加encoding参数解决编码问题
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self.base_dir), encoding='utf-8')
            
            # 输出WSL脚本的执行结果
            if result.stdout:
                self.logger.info(f"WSL脚本输出:\n{result.stdout}")
                print(result.stdout)
            
            if result.stderr:
                self.logger.warning(f"WSL脚本警告/错误信息:\n{result.stderr}")
                print(result.stderr)
            
            if result.returncode == 0:
                self.logger.info("WSL中执行拆分脚本完成")
                print("在WSL中执行拆分脚本完成")
                
                # 检查是否有生成的文件（在temp目录中）
                temp_dir = self.base_dir / "temp"
                if temp_dir.exists():
                    split_files = list(temp_dir.glob(f"era5_{year}{month:02d}*.nc"))
                    if split_files:
                        # 将生成的文件移动到正确的目录并重命名
                        moved_count = 0
                        for file_path in split_files:
                            # 从文件名提取日期和时间信息
                            filename = file_path.name
                            # 期望的格式: era5_YYYYMMDD_HH00.nc
                            match = re.search(r"era5_(\d{8}_\d{4})\.nc", filename)
                            if match:
                                datetime_part = match.group(1)
                                # 重命名为正确的格式
                                new_filename = f"era5_{datetime_part}_{data_type}.nc"
                                new_file_path = output_dir / new_filename
                                
                                # 移动文件，如果目标文件已存在则先删除
                                if new_file_path.exists():
                                    new_file_path.unlink()
                                shutil.move(str(file_path), str(new_file_path))
                                self.logger.info(f"移动文件: {file_path} -> {new_file_path}")
                                moved_count += 1
                        
                        self.logger.info(f"成功移动 {moved_count} 个拆分文件到 {output_dir}")
                        print(f"成功移动 {moved_count} 个拆分文件到 {output_dir}")
                        
                        # 清理temp目录中的文件
                        try:
                            for file_path in temp_dir.glob("era5_*.nc"):
                                file_path.unlink()
                            self.logger.info(f"清理temp目录中的文件")
                        except Exception as e:
                            self.logger.warning(f"清理temp目录失败: {e}")
                    else:
                        self.logger.warning(f"在temp目录中未找到生成的拆分文件")
                        print(f"警告: 在temp目录中未找到生成的拆分文件")
                else:
                    self.logger.warning("未找到temp目录")
                    print("警告: 未找到temp目录")
            else:
                self.logger.error(f"WSL脚本执行失败，返回码: {result.returncode}")
                raise subprocess.CalledProcessError(result.returncode, cmd)
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"执行WSL拆分脚本时出错: {e}")
            raise
        except Exception as e:
            self.logger.error(f"使用WSL拆分nc文件时出错: {e}")
            raise
        finally:
            # 清理临时文件
            if temp_nc_file.exists():
                temp_nc_file.unlink()
    
    def _get_days_for_month(self, year, month):
        """获取指定年月的天数列表"""
        days_in_month = calendar.monthrange(year, month)[1]
        return [f"{day:02d}" for day in range(1, days_in_month + 1)]
    
    def _generate_download_list(self):
        """生成要下载的年月列表"""
        if not all([self.start_year, self.start_month, self.end_year, self.end_month]):
            # 如果没有指定时间范围，默认下载2018年02月
            return [(2018, 2)]
        
        download_list = []
        year, month = self.start_year, self.start_month
        
        while (year < self.end_year) or (year == self.end_year and month <= self.end_month):
            download_list.append((year, month))
            
            # 移动到下一个月
            month += 1
            if month > 12:
                month = 1
                year += 1
                
        return download_list
    
    def run_complete_workflow(self):
        """运行完整的自动化工作流程"""
        workflow_start_time = datetime.now()
        self.logger.info("=== ERA5地表变量自动化工作流程开始 ===")
        
        try:
            print("=== ERA5地表变量自动化工作流程开始 ===")
            
            # 步骤1: 创建输出文件夹
            print("\n步骤1: 创建输出文件夹")
            self.logger.info("步骤1: 创建输出文件夹")
            self.create_output_directories()
            
            # 生成要处理的年月列表
            download_list = self._generate_download_list()
            
            # 顺序处理每个月份的数据
            for i, (year, month) in enumerate(download_list):
                print(f"\n处理 {year} 年 {month:02d} 月的数据")
                self.logger.info(f"开始处理 {year} 年 {month:02d} 月的数据")
                month_start_time = datetime.now()
                
                # 步骤2: 下载数据
                print(f"\n步骤2.{i+1}: 下载ERA5地表变量数据 ({year}-{month:02d})")
                self.logger.info(f"步骤2.{i+1}: 下载ERA5地表变量数据 ({year}-{month:02d})")
                download_start = datetime.now()
                zip_file_path = self.download_era5_data(year, month)
                download_time = (datetime.now() - download_start).total_seconds()
                self.logger.info(f"下载耗时: {download_time:.2f} 秒")
                
                # 步骤3: 解压并组织数据
                print(f"\n步骤3.{i+1}: 解压并组织数据 ({year}-{month:02d})")
                self.logger.info(f"步骤3.{i+1}: 解压并组织数据 ({year}-{month:02d})")
                organize_start = datetime.now()
                self.extract_and_organize_data(zip_file_path, year, month)
                organize_time = (datetime.now() - organize_start).total_seconds()
                self.logger.info(f"组织数据耗时: {organize_time:.2f} 秒")
                
                month_time = (datetime.now() - month_start_time).total_seconds()
                success_msg = f"{year} 年 {month:02d} 月数据处理完成，耗时: {month_time:.2f} 秒"
                self.logger.info(success_msg)
                print(success_msg)
            
            total_time = (datetime.now() - workflow_start_time).total_seconds()
            success_msg = f"自动化工作流程完成，总耗时: {total_time:.2f} 秒"
            self.logger.info(f"=== {success_msg} ===")
            print(f"\n=== {success_msg} ===")
            print(f"降水数据已保存到: {self.tp_dir}")
            print(f"地表变量数据已保存到: {self.sl_dir}")
                
        except Exception as e:
            error_msg = f"工作流程执行出错: {e}"
            self.logger.error(error_msg, exc_info=True)
            print(error_msg)
            raise


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='ERA5地表变量自动化下载和组织工具')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='设置日志级别')
    parser.add_argument('--base-dir', default='.', help='工作目录')
    parser.add_argument('--start-year', type=int, default=2018, help='起始年份 (默认: 2018)')
    parser.add_argument('--start-month', type=int, default=2, help='起始月份 (默认: 2)')
    parser.add_argument('--end-year', type=int, default=2018, help='结束年份 (默认: 2018)')
    parser.add_argument('--end-month', type=int, default=2, help='结束月份 (默认: 2)')
    
    args = parser.parse_args()
    
    # 将字符串转换为logging级别
    log_level = getattr(logging, args.log_level)
    
    workflow = ERA5SLAutomatedWorkflow(
        base_dir=args.base_dir, 
        log_level=log_level,
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month
    )
    workflow.run_complete_workflow()


if __name__ == "__main__":
    main()