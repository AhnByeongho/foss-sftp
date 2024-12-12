#!/bin/bash

# 오늘 날짜를 기준으로 target_date 설정
today_date=$(date +"%Y%m%d")

# 로그 디렉토리 및 파일 설정
log_dir="/Users/mac/work/foss-sftp/logs"
log_file="$log_dir/batch_$today_date.log"

mkdir -p "$log_dir"

# Python 스크립트 디렉토리 및 파일 설정
script_dir="/Users/mac/work/foss-sftp"
script_file="main.py"

# process_type 리스트 정의
process_types=(
    "RECEIVE_UNIVERSE"
    "RECEIVE_ACCOUNT"
    "RECEIVE_CUSTMERFND"
    "SEND_MPRATE"
    "SEND_MPLIST"
    "SEND_REBALCUS"
    "SEND_REPORT"
    "SEND_MP_INFO_EOF"
)

# process_type을 하나씩 실행
for process_type in "${process_types[@]}"; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting process_type: $process_type" >> "$log_file"
    
    # Python 스크립트 실행
    /usr/local/bin/python "$script_dir/$script_file" --target_date "$today_date" --process_type "$process_type" >> "$log_file" 2>&1

    # 실행 결과 확인
    if [ $? -ne 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Process failed for $process_type" >> "$log_file"
        exit 1
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Completed process_type: $process_type" >> "$log_file"
        echo "" >> "$log_file"
    fi
done

# 전체 프로세스 완료 로그 기록
echo "[$(date '+%Y-%m-%d %H:%M:%S')] All processes completed." >> "$log_file"
