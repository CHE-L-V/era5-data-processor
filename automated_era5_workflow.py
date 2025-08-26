#!/usr/bin/env python3
import os
import subprocess
import shutil
import cdsapi
import logging
import sys
from pathlib import Path
from datetime import datetime

class ERA5AutomatedWorkflow:
    def __init__(self, base_dir=".", log_level=logging.INFO, start_year=None, start_month=None, end_year=None, end_month=None):
        self.base_dir = Path(base_dir)
        self.pl_dir = self.base_dir / "pl"
        self.original_file = None
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
        log_file = log_dir / f"era5_workflow_{timestamp}.log"
        
        # 配置日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 创建logger
        self.logger = logging.getLogger('ERA5Workflow')
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
        
    def create_pl_directory(self):
        """创建pl文件夹（如果不存在）"""
        self.logger.info("开始创建pl目录")
        try:
            if not self.pl_dir.exists():
                self.pl_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"成功创建文件夹: {self.pl_dir}")
                print(f"创建文件夹: {self.pl_dir}")
            else:
                self.logger.info(f"文件夹已存在: {self.pl_dir}")
                print(f"文件夹已存在: {self.pl_dir}")
        except Exception as e:
            self.logger.error(f"创建pl目录失败: {e}")
            raise
    
    def download_era5_data(self):
        """下载ERA5数据"""
        self.logger.info("开始下载ERA5数据")
        
        # 生成要下载的年月列表
        download_list = self._generate_download_list()
        
        downloaded_files = []
        
        for year, month in download_list:
            # 生成文件名
            filename = f"era5_{year}{month:02d}.nc"
            file_path = self.base_dir / filename
            
            self.logger.info(f"处理 {year} 年 {month:02d} 月的数据")
            print(f"处理 {year} 年 {month:02d} 月的数据")
            
            # 检查文件是否已经存在
            if file_path.exists():
                file_size = file_path.stat().st_size
                self.logger.info(f"文件已存在，跳过下载: {file_path}, 文件大小: {file_size / (1024*1024):.2f} MB")
                print(f"文件已存在，跳过下载: {file_path}")
                downloaded_files.append(file_path)
                continue
            
            # 构建下载请求
            dataset = "reanalysis-era5-pressure-levels"
            request = {
                "product_type": ["reanalysis"],
                "variable": [
                    "geopotential",
                    "relative_humidity",
                    "temperature",
                    "u_component_of_wind",
                    "v_component_of_wind"
                ],
                "year": [str(year)],
                "month": [f"{month:02d}"],
                "day": self._get_days_for_month(year, month),
                "time": ["00:00", "06:00", "12:00", "18:00"],
                "pressure_level": [
                    "50", "100", "150", "200", "250", "300",
                    "400", "500", "600", "700", "850", "925", "1000"
                ],
                "data_format": "netcdf",
                "download_format": "unarchived"
            }

            self.logger.debug(f"下载请求参数: {request}")

            try:
                client = cdsapi.Client()
                
                self.logger.info(f"开始下载ERA5数据到 {file_path}")
                print(f"开始下载ERA5数据到 {file_path}")
                
                client.retrieve(dataset, request).download(str(file_path))
                
                # 检查文件是否成功下载
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    self.logger.info(f"下载完成: {file_path}, 文件大小: {file_size / (1024*1024):.2f} MB")
                    print(f"下载完成: {file_path}")
                    downloaded_files.append(file_path)
                else:
                    raise FileNotFoundError(f"下载的文件不存在: {file_path}")
                    
            except Exception as e:
                self.logger.error(f"下载ERA5数据失败: {e}")
                raise
        
        return downloaded_files
    
    def download_era5_data_single_month(self, year, month):
        """下载单个月份的ERA5数据"""
        self.logger.info(f"开始下载 {year} 年 {month:02d} 月的ERA5数据")
        
        # 生成文件名
        filename = f"era5_{year}{month:02d}.nc"
        file_path = self.base_dir / filename
        
        self.logger.info(f"处理 {year} 年 {month:02d} 月的数据")
        print(f"处理 {year} 年 {month:02d} 月的数据")
        
        # 检查文件是否已经存在
        if file_path.exists():
            file_size = file_path.stat().st_size
            self.logger.info(f"文件已存在，跳过下载: {file_path}, 文件大小: {file_size / (1024*1024):.2f} MB")
            print(f"文件已存在，跳过下载: {file_path}")
            return [file_path]
        
        # 构建下载请求
        dataset = "reanalysis-era5-pressure-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": [
                "geopotential",
                "relative_humidity",
                "temperature",
                "u_component_of_wind",
                "v_component_of_wind"
            ],
            "year": [str(year)],
            "month": [f"{month:02d}"],
            "day": self._get_days_for_month(year, month),
            "time": ["00:00", "06:00", "12:00", "18:00"],
            "pressure_level": [
                "50", "100", "150", "200", "250", "300",
                "400", "500", "600", "700", "850", "925", "1000"
            ],
            "data_format": "netcdf",
            "download_format": "unarchived"
        }

        self.logger.debug(f"下载请求参数: {request}")

        try:
            client = cdsapi.Client()
            
            self.logger.info(f"开始下载ERA5数据到 {file_path}")
            print(f"开始下载ERA5数据到 {file_path}")
            
            client.retrieve(dataset, request).download(str(file_path))
            
            # 检查文件是否成功下载
            if file_path.exists():
                file_size = file_path.stat().st_size
                self.logger.info(f"下载完成: {file_path}, 文件大小: {file_size / (1024*1024):.2f} MB")
                print(f"下载完成: {file_path}")
                return [file_path]
            else:
                raise FileNotFoundError(f"下载的文件不存在: {file_path}")
                
        except Exception as e:
            self.logger.error(f"下载ERA5数据失败: {e}")
            raise
    
    def split_data_to_pl_single_month(self, original_file, year, month):
        """将单个月份的数据拆分并保存到pl文件夹"""
        self.logger.info(f"开始拆分 {year} 年 {month:02d} 月的数据到pl文件夹")
        
        if not original_file or not original_file.exists():
            error_msg = f"原始文件不存在: {original_file}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        self.logger.info(f"开始拆分文件: {original_file}")
        print(f"开始拆分文件: {original_file}")
        
        # 保存原始文件路径供后续使用
        self.original_file = original_file
        
        # 首先尝试在WSL中执行拆分脚本
        wsl_success = self._split_with_wsl_single_month(year, month)
        
        # 如果WSL执行失败，回退到本地Python实现
        if not wsl_success:
            self.logger.warning("WSL中执行拆分脚本失败，回退到本地Python实现")
            print("WSL中执行拆分脚本失败，回退到本地Python实现")
            return self._split_with_python_single_month(year, month)
        
        return wsl_success
    
    def _split_with_wsl_single_month(self, year, month):
        """在WSL中执行拆分脚本（单个月份）"""
        try:
            # 检查WSL是否可用
            wsl_check = subprocess.run(["wsl", "--list", "--quiet"], 
                                      capture_output=True, text=True, timeout=10)
            
            if wsl_check.returncode != 0:
                self.logger.warning("WSL不可用，跳过WSL拆分")
                print("WSL不可用，跳过WSL拆分")
                return False
            
            # 生成输入文件名
            input_file = f"era5_{year}{month:02d}.nc"
            
            # 构建WSL命令，指定使用Ubuntu-24.04发行版，并传递输入文件名作为参数
            cmd = ["wsl", "-d", "Ubuntu-24.04", "-e", "bash", "./split_era5.sh", input_file]
            
            self.logger.info(f"执行WSL命令: {' '.join(cmd)}")
            print(f"在WSL中执行拆分脚本: split_era5.sh {input_file}")
            
            # 执行WSL命令，添加encoding='utf-8'以解决编码问题
            result = subprocess.run(cmd, capture_output=True, text=True, 
                                  cwd=str(self.base_dir), encoding='utf-8')
            
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
                
                # 检查是否有生成的文件
                pl_files = list(self.pl_dir.glob(f"era5_{year}{month:02d}*.nc"))
                if pl_files:
                    self.logger.info(f"成功生成 {len(pl_files)} 个拆分文件到 {self.pl_dir}")
                    print(f"成功生成 {len(pl_files)} 个拆分文件到 {self.pl_dir}")
                    return True
                else:
                    self.logger.warning("未在pl目录中找到生成的拆分文件")
                    print("警告: 未在pl目录中找到生成的拆分文件")
                    return False
            else:
                self.logger.error(f"WSL脚本执行失败，返回码: {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("WSL命令执行超时")
            print("WSL命令执行超时")
            return False
        except FileNotFoundError:
            self.logger.warning("未找到WSL命令，请确保已安装WSL")
            print("未找到WSL命令，请确保已安装WSL")
            return False
        except Exception as e:
            self.logger.error(f"执行WSL拆分脚本时发生未知错误: {e}")
            print(f"执行WSL拆分脚本时发生未知错误: {e}")
            return False
    
    def _split_with_python_single_month(self, year, month):
        """使用Python本地实现拆分数据（单个月份）"""
        self.logger.info(f"开始使用Python本地实现拆分 {year} 年 {month:02d} 月的数据")
        print(f"开始使用Python本地实现拆分 {year} 年 {month:02d} 月的数据")
        
        # 计算该月的总时间步数
        import calendar
        days_in_month = calendar.monthrange(year, month)[1]
        total_timesteps = days_in_month * 4  # 每天4个时间点
        
        success_count = 0
        error_count = 0
        
        # 拆分文件
        for i in range(1, total_timesteps + 1):
            # 计算日期和时间
            days = (i - 1) // 4 + 1
            hours = ((i - 1) % 4) * 6
            
            # 格式化
            day = f"{days:02d}"
            hour = f"{hours:02d}"
            
            # 生成输出文件名并保存到pl文件夹
            output_file = self.pl_dir / f"era5_{year}{month:02d}{day}_{hour}00.nc"
            
            # 使用cdo提取时间步
            cmd = ["cdo", "seltimestep,{}".format(i), str(self.original_file), str(output_file)]
            
            try:
                self.logger.debug(f"执行命令: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                if output_file.exists():
                    file_size = output_file.stat().st_size
                    self.logger.info(f"成功创建文件: {output_file} (时间步 {i}, 大小: {file_size / (1024*1024):.2f} MB)")
                    print(f"创建文件: {output_file} (时间步 {i})")
                    success_count += 1
                else:
                    self.logger.warning(f"文件创建后不存在: {output_file}")
                    
            except subprocess.CalledProcessError as e:
                error_msg = f"拆分文件时出错 (时间步 {i}): {e}"
                self.logger.error(f"{error_msg}, 标准错误: {e.stderr}")
                print(f"拆分文件时出错 (时间步 {i}): {e}")
                print(f"标准错误: {e.stderr}")
                error_count += 1
                continue
            except FileNotFoundError:
                error_msg = "错误: 未找到cdo命令。请确保已安装CDO (Climate Data Operators)"
                self.logger.error(error_msg)
                print(error_msg)
                print("安装命令: sudo apt-get install cdo 或 conda install -c conda-forge cdo")
                # 如果CDO不可用，我们无法继续拆分
                return False
            except Exception as e:
                self.logger.error(f"拆分文件时发生未知错误 (时间步 {i}): {e}")
                error_count += 1
                continue
        
        self.logger.info(f"数据拆分完成！成功: {success_count}, 失败: {error_count}")
        print("数据拆分完成！")
        
        return error_count == 0
    
    def cleanup_original_file_single_month(self, original_file):
        """删除单个月份的原始文件"""
        self.logger.info("开始清理原始文件")
        
        if original_file and original_file.exists():
            try:
                file_size = original_file.stat().st_size
                original_file.unlink()
                self.logger.info(f"成功删除原始文件: {original_file} (释放空间: {file_size / (1024*1024):.2f} MB)")
                print(f"已删除原始文件: {original_file}")
                return True
            except OSError as e:
                self.logger.error(f"删除原始文件时出错: {e}")
                print(f"删除原始文件时出错: {e}")
                return False
        else:
            self.logger.info("原始文件不存在，无需删除")
            print("原始文件不存在，无需删除")
            return True
    
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
    
    def _get_days_for_month(self, year, month):
        """获取指定年月的天数列表"""
        import calendar
        days_in_month = calendar.monthrange(year, month)[1]
        return [f"{day:02d}" for day in range(1, days_in_month + 1)]
    
    def split_data_to_pl(self):
        """将数据拆分并保存到pl文件夹（保持向后兼容）"""
        self.logger.info("开始拆分数据到pl文件夹")
        
        if not self.original_file or not self.original_file.exists():
            error_msg = f"原始文件不存在: {self.original_file}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        self.logger.info(f"开始拆分文件: {self.original_file}")
        print(f"开始拆分文件: {self.original_file}")
        
        # 从文件名提取年月信息
        filename = self.original_file.name
        year = int(filename[5:9])
        month = int(filename[9:11])
        
        # 首先尝试在WSL中执行拆分脚本
        wsl_success = self._split_with_wsl_single_month(year, month)
        
        # 如果WSL执行失败，回退到本地Python实现
        if not wsl_success:
            self.logger.warning("WSL中执行拆分脚本失败，回退到本地Python实现")
            print("WSL中执行拆分脚本失败，回退到本地Python实现")
            return self._split_with_python_single_month(year, month)
        
        return wsl_success
    
    def _split_with_wsl(self):
        """在WSL中执行拆分脚本（保持向后兼容）"""
        # 从文件名提取年月信息
        if self.original_file:
            filename = self.original_file.name
            year = int(filename[5:9])
            month = int(filename[9:11])
            return self._split_with_wsl_single_month(year, month)
        else:
            self.logger.error("原始文件路径未设置")
            return False
    
    def _split_with_python(self):
        """使用Python本地实现拆分数据（保持向后兼容）"""
        # 从文件名提取年月信息
        if self.original_file:
            filename = self.original_file.name
            year = int(filename[5:9])
            month = int(filename[9:11])
            return self._split_with_python_single_month(year, month)
        else:
            self.logger.error("原始文件路径未设置")
            return False
    
    def cleanup_original_file(self):
        """删除原始文件（保持向后兼容）"""
        self.logger.info("开始清理原始文件")
        
        if self.original_file and self.original_file.exists():
            try:
                file_size = self.original_file.stat().st_size
                self.original_file.unlink()
                self.logger.info(f"成功删除原始文件: {self.original_file} (释放空间: {file_size / (1024*1024):.2f} MB)")
                print(f"已删除原始文件: {self.original_file}")
                return True
            except OSError as e:
                self.logger.error(f"删除原始文件时出错: {e}")
                print(f"删除原始文件时出错: {e}")
                return False
        else:
            self.logger.info("原始文件不存在，无需删除")
            print("原始文件不存在，无需删除")
            return True
    
    def run_complete_workflow(self):
        """运行完整的自动化工作流程"""
        workflow_start_time = datetime.now()
        self.logger.info("=== ERA5自动化工作流程开始 ===")
        
        try:
            print("=== ERA5自动化工作流程开始 ===")
            
            # 步骤1: 创建pl文件夹
            print("\n步骤1: 创建pl文件夹")
            self.logger.info("步骤1: 创建pl文件夹")
            self.create_pl_directory()
            
            # 生成要处理的年月列表
            download_list = self._generate_download_list()
            
            # 如果只有一个月份，直接顺序处理
            if len(download_list) == 1:
                year, month = download_list[0]
                print(f"\n处理 {year} 年 {month:02d} 月的数据")
                self.logger.info(f"开始处理 {year} 年 {month:02d} 月的数据")
                month_start_time = datetime.now()
                
                # 下载数据
                print(f"\n步骤2.{month}: 下载ERA5数据 ({year}-{month:02d})")
                self.logger.info(f"步骤2.{month}: 下载ERA5数据 ({year}-{month:02d})")
                download_start = datetime.now()
                downloaded_files = self.download_era5_data_single_month(year, month)
                download_time = (datetime.now() - download_start).total_seconds()
                self.logger.info(f"下载耗时: {download_time:.2f} 秒")
                
                # 拆分数据
                print(f"\n步骤3.{month}: 拆分数据到pl文件夹 ({year}-{month:02d})")
                self.logger.info(f"步骤3.{month}: 拆分数据到pl文件夹 ({year}-{month:02d})")
                split_start = datetime.now()
                self.original_file = downloaded_files[0]
                split_success = self.split_data_to_pl_single_month(downloaded_files[0], year, month)
                split_time = (datetime.now() - split_start).total_seconds()
                self.logger.info(f"拆分耗时: {split_time:.2f} 秒")
                
                # 清理原始文件
                if split_success:
                    print(f"\n步骤4.{month}: 清理原始文件 ({year}-{month:02d})")
                    self.logger.info(f"步骤4.{month}: 清理原始文件 ({year}-{month:02d})")
                    self.cleanup_original_file_single_month(downloaded_files[0])
                    
                    month_time = (datetime.now() - month_start_time).total_seconds()
                    success_msg = f"{year} 年 {month:02d} 月数据处理完成，耗时: {month_time:.2f} 秒"
                    self.logger.info(success_msg)
                    print(success_msg)
                else:
                    error_msg = f"{year} 年 {month:02d} 月数据拆分过程中出现错误，保留原始文件"
                    self.logger.warning(error_msg)
                    print(error_msg)
            else:
                # 对于多个月份，使用流水线处理方式
                # 首先下载第一个月的数据
                first_year, first_month = download_list[0]
                print(f"\n处理 {first_year} 年 {first_month:02d} 月的数据")
                self.logger.info(f"开始处理 {first_year} 年 {first_month:02d} 月的数据")
                
                print(f"\n步骤2.{first_month}: 下载ERA5数据 ({first_year}-{first_month:02d})")
                self.logger.info(f"步骤2.{first_month}: 下载ERA5数据 ({first_year}-{first_month:02d})")
                download_start = datetime.now()
                first_downloaded_file = self.download_era5_data_single_month(first_year, first_month)[0]
                first_download_time = (datetime.now() - download_start).total_seconds()
                self.logger.info(f"下载耗时: {first_download_time:.2f} 秒")
                
                # 流水线处理后续月份
                prev_downloaded_file = first_downloaded_file
                prev_year, prev_month = first_year, first_month
                
                for i in range(1, len(download_list)):
                    current_year, current_month = download_list[i]
                    
                    # 同时进行：下载当前月份数据 + 拆分上一个月份数据
                    import threading
                    
                    # 下载当前月份数据
                    def download_current():
                        nonlocal current_downloaded_file
                        print(f"\n步骤2.{current_month}: 下载ERA5数据 ({current_year}-{current_month:02d})")
                        self.logger.info(f"步骤2.{current_month}: 下载ERA5数据 ({current_year}-{current_month:02d})")
                        download_start = datetime.now()
                        current_downloaded_file = self.download_era5_data_single_month(current_year, current_month)[0]
                        download_time = (datetime.now() - download_start).total_seconds()
                        self.logger.info(f"下载耗时: {download_time:.2f} 秒")
                    
                    # 拆分上一个月份数据
                    def split_previous():
                        print(f"\n步骤3.{prev_month}: 拆分数据到pl文件夹 ({prev_year}-{prev_month:02d})")
                        self.logger.info(f"步骤3.{prev_month}: 拆分数据到pl文件夹 ({prev_year}-{prev_month:02d})")
                        split_start = datetime.now()
                        self.original_file = prev_downloaded_file
                        split_success = self.split_data_to_pl_single_month(prev_downloaded_file, prev_year, prev_month)
                        split_time = (datetime.now() - split_start).total_seconds()
                        self.logger.info(f"拆分耗时: {split_time:.2f} 秒")
                        
                        # 清理上一个月的原始文件
                        if split_success:
                            print(f"\n步骤4.{prev_month}: 清理原始文件 ({prev_year}-{prev_month:02d})")
                            self.logger.info(f"步骤4.{prev_month}: 清理原始文件 ({prev_year}-{prev_month:02d})")
                            self.cleanup_original_file_single_month(prev_downloaded_file)
                    
                    # 并行执行下载和拆分
                    current_downloaded_file = None
                    download_thread = threading.Thread(target=download_current)
                    split_thread = threading.Thread(target=split_previous)
                    
                    download_thread.start()
                    split_thread.start()
                    
                    # 等待两个线程完成
                    download_thread.join()
                    split_thread.join()
                    
                    # 更新上一个月的数据
                    prev_downloaded_file = current_downloaded_file
                    prev_year, prev_month = current_year, current_month
                
                # 处理最后一个月的拆分
                print(f"\n步骤3.{prev_month}: 拆分数据到pl文件夹 ({prev_year}-{prev_month:02d})")
                self.logger.info(f"步骤3.{prev_month}: 拆分数据到pl文件夹 ({prev_year}-{prev_month:02d})")
                split_start = datetime.now()
                self.original_file = prev_downloaded_file
                split_success = self.split_data_to_pl_single_month(prev_downloaded_file, prev_year, prev_month)
                split_time = (datetime.now() - split_start).total_seconds()
                self.logger.info(f"拆分耗时: {split_time:.2f} 秒")
                
                # 清理最后一个月的原始文件
                if split_success:
                    print(f"\n步骤4.{prev_month}: 清理原始文件 ({prev_year}-{prev_month:02d})")
                    self.logger.info(f"步骤4.{prev_month}: 清理原始文件 ({prev_year}-{prev_month:02d})")
                    self.cleanup_original_file_single_month(prev_downloaded_file)
            
            total_time = (datetime.now() - workflow_start_time).total_seconds()
            success_msg = f"自动化工作流程完成，总耗时: {total_time:.2f} 秒"
            self.logger.info(f"=== {success_msg} ===")
            print(f"\n=== {success_msg} ===")
            print(f"所有拆分文件已保存到: {self.pl_dir}")
                
        except Exception as e:
            error_msg = f"工作流程执行出错: {e}"
            self.logger.error(error_msg, exc_info=True)
            print(error_msg)
            print("保留原始文件以便手动处理")
            raise

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ERA5自动化下载和拆分工具')
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
    
    workflow = ERA5AutomatedWorkflow(
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