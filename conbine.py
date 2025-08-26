import os
import numpy as np
import xarray as xr
from pathlib import Path
from datetime import datetime


def check_data_info(pl_path, sl_path, tp_path):
    """查看三个文件夹中nc文件的基本信息"""
    pl_dir = Path(pl_path)
    sl_dir = Path(sl_path)
    tp_dir = Path(tp_path)
    
    # 获取文件列表
    pl_files = sorted(pl_dir.glob("*.nc"))
    sl_files = sorted(sl_dir.glob("*.nc"))
    tp_files = sorted(tp_dir.glob("*.nc"))
    
    print(f"压力层文件数量: {len(pl_files)}")
    print(f"地表层文件数量: {len(sl_files)}")
    print(f"降水文件数量: {len(tp_files)}")
    
    # 检查第一个pl文件
    if pl_files:
        print(f"\n检查压力层文件: {pl_files[0].name}")
        ds = xr.open_dataset(pl_files[0])
        print(f"维度: {dict(ds.sizes)}")
        print(f"变量: {list(ds.data_vars.keys())}")
        for var in ds.data_vars:
            print(f"  {var}: {ds[var].shape}")
        ds.close()
    
    # 检查第一个sl文件
    if sl_files:
        print(f"\n检查地表层文件: {sl_files[0].name}")
        ds = xr.open_dataset(sl_files[0])
        print(f"维度: {dict(ds.sizes)}")
        print(f"变量: {list(ds.data_vars.keys())}")
        for var in ds.data_vars:
            print(f"  {var}: {ds[var].shape}")
        ds.close()
    
    # 检查第一个tp文件
    if tp_files:
        print(f"\n检查降水文件: {tp_files[0].name}")
        ds = xr.open_dataset(tp_files[0])
        print(f"维度: {dict(ds.sizes)}")
        print(f"变量: {list(ds.data_vars.keys())}")
        for var in ds.data_vars:
            print(f"  {var}: {ds[var].shape}")
        ds.close()
    
    return pl_files, sl_files, tp_files


def extract_datetime_from_filename(filename):
    """从文件名中提取日期时间信息"""
    # 移除文件扩展名
    name = filename.stem
    
    # 如果文件名包含日期时间格式，提取它
    # 例如：era5_20180101_0000_pl.nc -> 20180101_0000
    parts = name.split('_')
    
    # 查找日期和时间部分
    date_part = None
    time_part = None
    
    for part in parts:
        # 查找8位数字的日期 (YYYYMMDD)
        if len(part) == 8 and part.isdigit():
            date_part = part
        # 查找4位数字的时间 (HHMM)
        elif len(part) == 4 and part.isdigit():
            time_part = part
    
    if date_part and time_part:
        return f"{date_part}_{time_part}"
    else:
        # 如果找不到标准格式，返回去掉类型后缀的文件名
        # 移除最后的 _pl, _sl, _tp 等后缀
        if name.endswith('_pl') or name.endswith('_sl') or name.endswith('_tp'):
            return '_'.join(parts[:-1])
        return name


def parse_datetime_string(datetime_str):
    """将日期时间字符串解析为datetime对象"""
    try:
        # 格式: YYYYMMDD_HHMM
        return datetime.strptime(datetime_str, "%Y%m%d_%H%M")
    except ValueError:
        try:
            # 格式: YYYYMMDD_HH00 (如果分钟是00)
            return datetime.strptime(datetime_str, "%Y%m%d_%H00")
        except ValueError:
            # 如果解析失败，返回None
            return None


def merge_data(pl_path, sl_path, tp_path, output_path, start_time=None, end_time=None):
    """合并三个文件夹中的数据并保存到输出目录
    
    Args:
        pl_path: 压力层文件路径
        sl_path: 地表层文件路径
        tp_path: 降水文件路径
        output_path: 输出路径
        start_time: 开始时间 (格式: "YYYY-MM-DD HH:MM" 或 "YYYYMMDD_HHMM")
        end_time: 结束时间 (格式: "YYYY-MM-DD HH:MM" 或 "YYYYMMDD_HHMM")
    """
    pl_dir = Path(pl_path)
    sl_dir = Path(sl_path)
    tp_dir = Path(tp_path)
    output_dir = Path(output_path)
    
    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 获取文件列表
    pl_files = sorted(pl_dir.glob("*.nc"))
    sl_files = sorted(sl_dir.glob("*.nc"))
    tp_files = sorted(tp_dir.glob("*.nc"))
    
    # 解析起止时间
    start_dt = None
    end_dt = None
    
    if start_time:
        # 尝试解析不同的时间格式
        try:
            # 格式: "YYYY-MM-DD HH:MM"
            start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
        except ValueError:
            try:
                # 格式: "YYYYMMDD_HHMM"
                start_dt = datetime.strptime(start_time, "%Y%m%d_%H%M")
            except ValueError:
                print(f"无法解析开始时间: {start_time}")
                return
    
    if end_time:
        try:
            # 格式: "YYYY-MM-DD HH:MM"
            end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M")
        except ValueError:
            try:
                # 格式: "YYYYMMDD_HHMM"
                end_dt = datetime.strptime(end_time, "%Y%m%d_%H%M")
            except ValueError:
                print(f"无法解析结束时间: {end_time}")
                return
    
    # 按日期时间分组
    date_groups = {}
    
    print("\n=== 文件名分析 ===")
    
    # 处理所有文件并过滤时间范围
    all_files = []
    
    # 处理pl文件
    print("PL文件:")
    for file in pl_files:
        datetime_str = extract_datetime_from_filename(file)
        file_dt = parse_datetime_string(datetime_str)
        if file_dt:
            # 检查是否在时间范围内
            if start_dt and file_dt < start_dt:
                continue
            if end_dt and file_dt > end_dt:
                continue
            all_files.append(('pl', file, datetime_str, file_dt))
            print(f"  {file.name} -> {datetime_str}")
    
    # 处理sl文件
    print("SL文件:")
    for file in sl_files:
        datetime_str = extract_datetime_from_filename(file)
        file_dt = parse_datetime_string(datetime_str)
        if file_dt:
            # 检查是否在时间范围内
            if start_dt and file_dt < start_dt:
                continue
            if end_dt and file_dt > end_dt:
                continue
            all_files.append(('sl', file, datetime_str, file_dt))
            print(f"  {file.name} -> {datetime_str}")
    
    # 处理tp文件
    print("TP文件:")
    for file in tp_files:
        datetime_str = extract_datetime_from_filename(file)
        file_dt = parse_datetime_string(datetime_str)
        if file_dt:
            # 检查是否在时间范围内
            if start_dt and file_dt < start_dt:
                continue
            if end_dt and file_dt > end_dt:
                continue
            all_files.append(('tp', file, datetime_str, file_dt))
            print(f"  {file.name} -> {datetime_str}")
    
    # 按日期时间分组
    for file_type, file, datetime_str, file_dt in all_files:
        if datetime_str not in date_groups:
            date_groups[datetime_str] = {'pl': None, 'sl': None, 'tp': None}
        date_groups[datetime_str][file_type] = file
    
    print(f"找到 {len(date_groups)} 个日期时间组 (在指定时间范围内)")
    
    # 显示时间范围信息
    if start_dt or end_dt:
        print(f"时间范围: {start_dt} 到 {end_dt}")
    
    processed_count = 0
    
    # 处理每个日期时间组
    for datetime_str, files in date_groups.items():
        if files['pl'] is None or files['sl'] is None or files['tp'] is None:
            print(f"跳过 {datetime_str}：缺少pl、sl或tp文件")
            continue
        
        print(f"处理 {datetime_str}...")
        
        try:
            all_data = []
            var_names = []  # 保存变量名
            
            # 定义标准气压层（hPa）
            pressure_levels = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
            
            # 处理压力层文件
            ds_pl = xr.open_dataset(files['pl'])
            pl_vars = list(ds_pl.data_vars.keys())
            print(f"  PL变量: {pl_vars}")
            
            for var in pl_vars:
                data = ds_pl[var].values
                # 去掉时间维度
                if data.ndim == 4:  # (time, level, lat, lon)
                    data = data.squeeze(0)  # (level, lat, lon)
                    # 为每个压力层添加变量名
                    for level_idx in range(data.shape[0]):
                        all_data.append(data[level_idx])  # (lat, lon)
                        pressure = pressure_levels[level_idx]
                        var_names.append(f"{var}{pressure}")
            
            # 处理地表层文件
            ds_sl = xr.open_dataset(files['sl'])
            sl_vars = list(ds_sl.data_vars.keys())
            print(f"  SL变量: {sl_vars}")
            
            for var in sl_vars:
                data = ds_sl[var].values
                # 去掉时间维度
                if data.ndim == 3:  # (time, lat, lon)
                    data = data.squeeze(0)  # (lat, lon)
                all_data.append(data)
                var_names.append(var)
            
            # 处理降水文件
            ds_tp = xr.open_dataset(files['tp'])
            tp_vars = list(ds_tp.data_vars.keys())
            print(f"  TP变量: {tp_vars}")
            
            for var in tp_vars:
                data = ds_tp[var].values
                # 去掉时间维度
                if data.ndim == 3:  # (time, lat, lon)
                    data = data.squeeze(0)  # (lat, lon)
                all_data.append(data)
                var_names.append(var)
            
            # 堆叠所有数据为3D数组
            merged = np.stack(all_data, axis=0)  # (levels, lat, lon)
            actual_levels = merged.shape[0]
            print(f"  合并后形状: {merged.shape}")
            print(f"  变量总数: {len(var_names)}")
            print(f"  变量名示例: {var_names[:10]}...")
            
            # 创建包含所有变量的数据集
            data_vars = {}
            coords = {
                'latitude': ds_pl.latitude.values,
                'longitude': ds_pl.longitude.values
            }
            
            # 为每个变量创建单独的DataArray
            for i, var_name in enumerate(var_names):
                data_vars[var_name] = (['latitude', 'longitude'], merged[i])
            
            # 创建新的xarray数据集
            merged_ds = xr.Dataset(data_vars, coords=coords)
            
            # 添加属性信息
            merged_ds.attrs = {
                'description': 'Merged ERA5 data (pl + sl + tp)',
                'total_variables': len(var_names),
                'pressure_levels': ', '.join(map(str, pressure_levels)),
                'pl_variables': ', '.join(pl_vars),
                'sl_variables': ', '.join(sl_vars),
                'tp_variables': ', '.join(tp_vars),
                'variable_list': ', '.join(var_names),
                'datetime': datetime_str
            }
            
            # 保存为nc文件
            output_file = output_dir / f"era5_{datetime_str}.nc"
            merged_ds.to_netcdf(output_file, unlimited_dims=None)
            
            # 验证保存的文件
            verify_ds = xr.open_dataset(output_file)
            print(f"  保存到: {output_file}")
            print(f"  验证变量数: {len(verify_ds.data_vars)}")
            print(f"  变量名示例: {list(verify_ds.data_vars.keys())[:10]}...")
            
            # 检查第一个变量的形状
            first_var = list(verify_ds.data_vars.keys())[0]
            var_shape = verify_ds[first_var].shape
            print(f"  单个变量形状: {var_shape}")
            
            verify_ds.close()
            
            # 关闭数据集
            ds_pl.close()
            ds_sl.close()
            ds_tp.close()
            merged_ds.close()
            
            processed_count += 1
            
        except Exception as e:
            print(f"处理 {datetime_str} 时出错: {e}")
    
    print(f"\n合并完成！成功处理 {processed_count} 个文件")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='合并ERA5数据文件')
    parser.add_argument('--pl-path', default='pl', help='压力层文件路径')
    parser.add_argument('--sl-path', default='sl', help='地表层文件路径')
    parser.add_argument('--tp-path', default='tp', help='降水文件路径')
    parser.add_argument('--output-path', default='data', help='输出路径')
    parser.add_argument('--start-time', help='开始时间 (格式: "YYYY-MM-DD HH:MM" 或 "YYYYMMDD_HHMM")')
    parser.add_argument('--end-time', help='结束时间 (格式: "YYYY-MM-DD HH:MM" 或 "YYYYMMDD_HHMM")')
    
    args = parser.parse_args()
    
    # 先查看数据信息
    print("=== 查看数据信息 ===")
    pl_files, sl_files, tp_files = check_data_info(args.pl_path, args.sl_path, args.tp_path)
    
    # 合并数据并保存到输出目录
    print("\n=== 合并数据 ===")
    merge_data(args.pl_path, args.sl_path, args.tp_path, args.output_path, 
               args.start_time, args.end_time)


if __name__ == "__main__":
    main()
