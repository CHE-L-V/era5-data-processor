#!/usr/bin/env python3
"""
测试conbine.py是否可以正常导入和运行
"""

try:
    # 测试导入
    import sys
    from pathlib import Path
    
    # 添加当前目录到Python路径
    sys.path.append(str(Path(__file__).parent))
    
    # 尝试导入conbine模块
    import conbine
    
    print("conbine.py 导入成功！")
    print("可用函数:")
    print("- check_data_info(pl_path, sl_path, tp_path)")
    print("- extract_datetime_from_filename(filename)")
    print("- parse_datetime_string(datetime_str)")
    print("- merge_data(pl_path, sl_path, tp_path, output_path, start_time=None, end_time=None)")
    
    # 显示帮助信息
    print("\n使用帮助:")
    print("python conbine.py --help")
    
except SyntaxError as e:
    print(f"语法错误: {e}")
    print("请检查conbine.py文件中的语法问题")
    
except Exception as e:
    print(f"导入错误: {e}")