#!/usr/bin/env python3
"""
ERA5项目日志配置模块

提供统一的日志配置功能，支持文件和控制台输出
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


class LoggingManager:
    """日志管理器"""
    
    def __init__(self, name="ERA5Project", base_dir=".", log_level=logging.INFO):
        self.name = name
        self.base_dir = Path(base_dir)
        self.log_level = log_level
        self.logger = None
        self.log_file = None
        
    def setup_logging(self, log_prefix=None):
        """设置日志配置
        
        Args:
            log_prefix (str): 日志文件前缀，如果为None则使用默认前缀
        """
        # 创建日志目录
        log_dir = self.base_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # 生成日志文件名（包含时间戳）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if log_prefix:
            log_filename = f"{log_prefix}_{timestamp}.log"
        else:
            log_filename = f"{self.name.lower()}_{timestamp}.log"
        
        self.log_file = log_dir / log_filename
        
        # 配置日志格式
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # 创建logger
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.log_level)
        
        # 清除现有的处理器
        self.logger.handlers.clear()
        
        # 文件处理器 - 详细格式
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(detailed_formatter)
        
        # 控制台处理器 - 简化格式
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(simple_formatter)
        
        # 添加处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"日志系统初始化完成")
        self.logger.info(f"日志文件: {self.log_file}")
        self.logger.info(f"工作目录: {self.base_dir.absolute()}")
        self.logger.info(f"日志级别: {logging.getLevelName(self.log_level)}")
        
        return self.logger
    
    def get_logger(self):
        """获取logger实例"""
        if self.logger is None:
            self.setup_logging()
        return self.logger
    
    def log_system_info(self):
        """记录系统信息"""
        import platform
        import os
        
        if self.logger is None:
            self.setup_logging()
            
        self.logger.info("=== 系统信息 ===")
        self.logger.info(f"操作系统: {platform.system()} {platform.release()}")
        self.logger.info(f"Python版本: {platform.python_version()}")
        self.logger.info(f"当前用户: {os.getenv('USER', 'Unknown')}")
        self.logger.info(f"主机名: {platform.node()}")
        
    def log_performance(self, operation_name, start_time, end_time=None):
        """记录性能信息
        
        Args:
            operation_name (str): 操作名称
            start_time (datetime): 开始时间
            end_time (datetime): 结束时间，如果为None则使用当前时间
        """
        if end_time is None:
            end_time = datetime.now()
            
        duration = (end_time - start_time).total_seconds()
        
        if self.logger is None:
            self.setup_logging()
            
        self.logger.info(f"性能统计 - {operation_name}: {duration:.2f} 秒")
        
        # 根据耗时选择不同的日志级别
        if duration > 300:  # 5分钟
            self.logger.warning(f"{operation_name} 耗时较长: {duration:.2f} 秒")
        elif duration > 60:  # 1分钟
            self.logger.info(f"{operation_name} 耗时: {duration:.2f} 秒")
        else:
            self.logger.debug(f"{operation_name} 耗时: {duration:.2f} 秒")


def get_default_logger(name="ERA5Project", log_level=logging.INFO):
    """获取默认配置的logger
    
    Args:
        name (str): logger名称
        log_level: 日志级别
        
    Returns:
        logging.Logger: 配置好的logger实例
    """
    manager = LoggingManager(name=name, log_level=log_level)
    return manager.setup_logging()


def setup_era5_logging(log_level=logging.INFO, log_prefix=None):
    """为ERA5项目设置统一的日志配置
    
    Args:
        log_level: 日志级别
        log_prefix (str): 日志文件前缀
        
    Returns:
        tuple: (logger, log_file_path)
    """
    manager = LoggingManager(name="ERA5Project", log_level=log_level)
    logger = manager.setup_logging(log_prefix)
    manager.log_system_info()
    
    return logger, manager.log_file


# 便利函数
def create_workflow_logger(log_level=logging.INFO):
    """创建工作流日志器"""
    return setup_era5_logging(log_level, "era5_workflow")


def create_download_logger(log_level=logging.INFO):
    """创建下载日志器"""
    return setup_era5_logging(log_level, "era5_download")


if __name__ == "__main__":
    # 测试日志配置
    logger, log_file = setup_era5_logging()
    logger.info("日志配置测试成功")
    logger.debug("这是调试信息")
    logger.warning("这是警告信息")
    logger.error("这是错误信息")
    print(f"日志文件位置: {log_file}")