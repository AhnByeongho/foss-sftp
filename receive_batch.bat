@echo off
REM 오늘 날짜를 기준으로 target_date 설정
for /f "tokens=2 delims==" %%i in ('"wmic os get localdatetime /value"') do set today_date=%%i
set today_date=%today_date:~0,8%

REM 로그 디렉토리 및 파일 설정
set log_dir=D:\QBS_PROJECT\foss-sftp\logs
set log_file=%log_dir%\batch_%today_date%.log

REM 로그 디렉토리 생성
if not exist "%log_dir%" mkdir "%log_dir%"

REM Python 스크립트 디렉토리 및 파일 설정
set script_dir=D:\QBS_PROJECT\foss-sftp
set script_file=main.py

REM process_type 리스트 정의
set process_types=DELETE_OLDDATA RECEIVE_UNIVERSE RECEIVE_ACCOUNT RECEIVE_CUSTMERFND

REM process_type을 하나씩 실행
for %%p in (%process_types%) do (
    echo [%date% %time%] Starting process_type: %%p >> "%log_file%"
    
    REM Python 스크립트 실행
    python "%script_dir%\%script_file%" --target_date "%today_date%" --process_type "%%p" >> "%log_file%" 2>&1

    REM 실행 결과 확인
    if errorlevel 1 (
        echo [%date% %time%] ERROR: Process failed for %%p >> "%log_file%"
        exit /b 1
    ) else (
        echo [%date% %time%] Completed process_type: %%p >> "%log_file%"
        echo. >> "%log_file%"
    )
)

REM 전체 프로세스 완료 로그 기록
echo [%date% %time%] All processes completed. >> "%log_file%"
echo. >> "%log_file%"
echo. >> "%log_file%"
echo. >> "%log_file%"
