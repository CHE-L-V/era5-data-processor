#!/bin/bash
# 拆分ERA5地表变量数据文件脚本
# Split ERA5 surface level data file script

# 设置工作目录为脚本所在目录
cd "$(dirname "$0")"

# 检查是否提供了输入文件参数
if [ $# -eq 0 ]; then
    echo "错误: 请提供输入文件名作为参数"
    echo "Error: Please provide input filename as argument"
    echo "用法: ./split_era5_sl.sh <input_file>"
    echo "Usage: ./split_era5_sl.sh <input_file>"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_DIR="temp"

echo "=== ERA5地表变量数据拆分脚本 ==="
echo "当前工作目录: $(pwd)"
echo "用户: $(whoami)"
echo "系统信息: $(uname -a)"
echo "==============================="

# 检查CDO是否安装
if ! command -v cdo &> /dev/null; then
    echo "错误: 未找到CDO命令"
    echo "Error: CDO command not found"
    echo "请安装CDO: sudo apt-get install cdo"
    exit 1
fi

echo "CDO版本: $(cdo -V 2>&1 | head -n 1)"

# 检查输入文件是否存在
if [ ! -f "$INPUT_FILE" ]; then
    echo "错误: 找不到输入文件 $INPUT_FILE"
    echo "Error: Input file $INPUT_FILE not found"
    echo "当前目录文件列表:"
    ls -la
    exit 1
fi

# 从文件名提取年月信息
filename=$(basename "$INPUT_FILE")

# 从环境变量获取年月（由Python脚本设置）
if [ -n "$ERA5_YEAR" ] && [ -n "$ERA5_MONTH" ]; then
    year=$ERA5_YEAR
    month=$ERA5_MONTH
    echo "从环境变量获取年月信息: 年=$year, 月=$month"
else
    # 尝试从文件名提取年月信息（兼容旧版本）
    # 注意：对于地表变量数据，文件名格式不同，这种方法可能不适用
    echo "警告: 无法从环境变量获取年月信息"
    year=2018
    month=02
    echo "使用默认年月信息: 年=$year, 月=$month"
fi

# 检查年月是否有效
if [[ ! $year =~ ^[0-9]{4}$ ]] || [[ ! $month =~ ^[0-9]{2}$ ]]; then
    echo "错误: 无效的年月信息: 年=$year, 月=$month"
    echo "Error: Invalid year/month: year=$year, month=$month"
    exit 1
fi

# 计算该月的天数
case $month in
    01|03|05|07|08|10|12) days_in_month=31 ;;
    04|06|09|11) days_in_month=30 ;;
    02) 
        # 判断是否为闰年
        if (( year % 400 == 0 )) || (( year % 4 == 0 && year % 100 != 0 )); then
            days_in_month=29
        else
            days_in_month=28
        fi
        ;;
    *) 
        echo "错误: 无效的月份: $month"
        echo "Error: Invalid month: $month"
        exit 1
        ;;
esac

# 计算总时间步数 (每天4个时间点)
total_timesteps=$((days_in_month * 4))

echo "输入文件: $INPUT_FILE (年: $year, 月: $month, 天数: $days_in_month)"
echo "总时间步数: $total_timesteps"

# 检查输入文件大小
file_size=$(stat -c%s "$INPUT_FILE" 2>/dev/null || stat -f%z "$INPUT_FILE" 2>/dev/null)
file_size_mb=$(echo "scale=2; $file_size / 1024 / 1024" | bc 2>/dev/null || echo "N/A")
echo "输入文件大小: ${file_size_mb} MB"

# 创建输出目录
mkdir -p "$OUTPUT_DIR"
echo "输出目录: $OUTPUT_DIR"

echo "开始拆分文件: $INPUT_FILE"
echo "Starting to split file: $INPUT_FILE"

# 初始化计数器
success_count=0
error_count=0

# 拆分文件
for i in $(seq 1 $total_timesteps); do
    # 计算对应的日期和时间
    days=$(( (i-1) / 4 + 1 ))
    hours=$(( ((i-1) % 4) * 6 ))

    # 格式化日期和时间
    day=$(printf "%02d" $days)
    hour=$(printf "%02d" $hours)

    # 生成输出文件名（保存到pl目录）
    output_file="${OUTPUT_DIR}/era5_${year}${month}${day}_${hour}00.nc"

    # 提取对应时间步
    echo "处理时间步 $i -> $output_file"
    if cdo seltimestep,$i $INPUT_FILE $output_file; then
        # 检查文件是否成功创建
        if [ -f "$output_file" ]; then
            file_size=$(stat -c%s "$output_file" 2>/dev/null || stat -f%z "$output_file" 2>/dev/null)
            file_size_mb=$(echo "scale=2; $file_size / 1024 / 1024" | bc 2>/dev/null || echo "N/A")
            echo "成功创建文件: $output_file (时间步 $i, 大小: ${file_size_mb} MB)"
            ((success_count++))
        else
            echo "警告: 文件创建失败: $output_file (时间步 $i)"
            ((error_count++))
        fi
    else
        echo "错误: CDO命令执行失败 (时间步 $i)"
        ((error_count++))
    fi
done

echo "拆分完成！"
echo "Split completed!"

# 显示生成的文件数量
file_count=$(ls -1 "$OUTPUT_DIR"/era5_${year}${month}*.nc 2>/dev/null | wc -l)
echo "总共生成了 $file_count 个文件 (成功: $success_count, 失败: $error_count)"
echo "Total files generated: $file_count (Success: $success_count, Error: $error_count)"

# 列出前10个生成的文件
echo "前10个生成的文件:"
ls -lh "$OUTPUT_DIR"/era5_${year}${month}*.nc 2>/dev/null | head -n 10