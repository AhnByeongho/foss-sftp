import argparse
import paramiko
import json
import os
import sys
from sqlalchemy import create_engine
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import (
    delete_old_bcp_data,
    insert_fnd_list_data,
    insert_fnd_list_data_to_qbt_api,
    insert_customer_account_data,
    insert_customer_fund_data,
    process_yesterday_return_data,
    process_mp_list,
    process_rebalcus,
    process_report,
    process_mp_info_eof,
    log_message,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_dir, "config.json")

# sftp & DB config.json
with open(config_path, "r") as config_file:
    config = json.load(config_file)


def get_sftp_connection(process_type):
    if process_type in ["RECEIVE_UNIVERSE", "RECEIVE_ACCOUNT", "RECEIVE_CUSTMERFND"]:
        sftp_config = config["sftp"]["foss"]
    else:
        sftp_config = config["sftp"]["fossDev"]
    transport = paramiko.Transport((sftp_config["host"], sftp_config["port"]))
    transport.connect(username=sftp_config["user"], password=sftp_config["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def get_sqlalchemy_connection(env):
    db_config = config["databases"][env]
    # MariaDB
    if env.startswith("qbt_api"):
        connection_url = (
            f"mysql+pymysql://{db_config['username']}:{db_config['password']}@"
            f"{db_config['server']}/{db_config['database']}"
        )
    # MS SQL Server
    else:
        connection_url = (
            f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@"
            f"{db_config['server']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
        )
    return create_engine(connection_url)


def main():
    parser = argparse.ArgumentParser(description="Batch Process Script")
    parser.add_argument(
        "--target_date", required=True, help="Target date for processing"
    )
    parser.add_argument(
        "--process_type", required=True, help="Type of process to execute"
    )
    parser.add_argument(
        "--manual_customer_ids",
        help="Comma-separated list of customer IDs for manual rebalancing",
    )
    parser.add_argument("--manual_rebal_yn", help="Manual rebalancing flag (Y or N)")
    parser.add_argument("--forced_rebal_date", help="Forced rebalancing date")

    args = parser.parse_args()

    target_date = args.target_date
    process_type = args.process_type

    try:
        # SFTP 연결
        sftp, transport = get_sftp_connection(process_type)

        engine = get_sqlalchemy_connection(env="dev")
        engine_qbt_api = get_sqlalchemy_connection(env="qbt_api_dev")

        # 엔진에서 연결 생성
        with engine.connect() as connection:
            # foss_data directory 접근
            sftp.chdir("foss_data")

            try:
                files_to_read = [
                    filename for filename in sftp.listdir() if target_date in filename
                ]  # fnd_list, ap_acc_info, ap_fnd_info

                # 파일 내용 읽기
                file_contents = {}
                for file_name in files_to_read:
                    key = file_name.split(".")[0]
                    with sftp.file(file_name, "r") as file_stream:
                        content = file_stream.read().decode("utf-8")
                        file_contents[key] = content

                fnd_list = file_contents.get("fnd_list")  # TBL_FOSS_UNIVERSE
                ap_acc_info = file_contents.get(
                    "ap_acc_info"
                )  # TBL_FOSS_CUSTOMERACCOUNT
                ap_fnd_info = file_contents.get("ap_fnd_info")  # TBL_FOSS_CUSTOMERFUND

                # ------------------ 1개월 전 데이터 삭제 (TBL_FOSS_BCPDATA) ------------------ #
                if process_type == "DELETE_OLDDATA":
                    delete_old_bcp_data(connection)

                # ------------------------------ 유니버스 수신 ------------------------------- #
                elif process_type == "RECEIVE_UNIVERSE":  # TBL_FOSS_UNIVERSE
                    if fnd_list:
                        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[
                            :-3
                        ]
                        insert_fnd_list_data(
                            connection, fnd_list, target_date, start_time
                        )
                        with engine_qbt_api.connect() as connection_qbt_api:
                            insert_fnd_list_data_to_qbt_api(
                                connection, connection_qbt_api, target_date
                            )
                    else:
                        log_message(f"No data fnd_list found for {target_date}.")

                # ---------------------------- 고객 계좌 정보 수신 ---------------------------- #
                elif process_type == "RECEIVE_ACCOUNT":  # TBL_FOSS_CUSTOMERACCOUNT
                    if ap_acc_info:
                        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[
                            :-3
                        ]
                        insert_customer_account_data(
                            connection, ap_acc_info, target_date, start_time
                        )
                    else:
                        log_message(f"No data ap_acc_info found for {target_date}.")

                # --------------------------- 고객 보유펀드 정보 수신 --------------------------- #
                elif process_type == "RECEIVE_CUSTMERFND":  # TBL_FOSS_CUSTOMERFUND
                    if ap_fnd_info:
                        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[
                            :-3
                        ]
                        insert_customer_fund_data(
                            connection, ap_fnd_info, target_date, start_time
                        )
                    else:
                        log_message(f"No data ap_fnd_info found for {target_date}.")

                # ----------- 전일 수익률 송신 처리(최근 영업일에 수익률 자료가 있을때만 생성) ------------ #
                elif process_type == "SEND_MPRATE":  # mp_info
                    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    process_yesterday_return_data(
                        connection, target_date, sftp, start_time
                    )

                # ----------------------------- MP 리스트 송신 처리 ---------------------------- #
                elif process_type == "SEND_MPLIST":  # mp_fnd_info
                    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    process_mp_list(connection, target_date, sftp, start_time)

                # --------------------------- 리밸런싱 고객자료 송신 처리 ------------------------- #
                elif process_type == "SEND_REBALCUS":  # ap_reval_yn
                    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                    # 리밸런싱 송신 처리
                    process_rebalcus(
                        connection,
                        target_date,
                        sftp,
                        start_time=start_time,
                        manual_customer_ids=args.manual_customer_ids
                        if hasattr(args, "manual_customer_ids")
                        else None,
                        manual_rebal_yn=args.manual_rebal_yn
                        if hasattr(args, "manual_rebal_yn")
                        else None,
                        forced_rebal_date=args.forced_rebal_date
                        if hasattr(args, "forced_rebal_date")
                        else None,
                    )

                # ------------------------------ 리포트 송신 처리 ------------------------------- #
                elif process_type == "SEND_REPORT":  # report
                    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    process_report(connection, target_date, sftp, start_time)

                # ------------------------- MP_INFO_EOF 빈파일 송신 처리 ------------------------ #
                elif process_type == "SEND_MP_INFO_EOF":  # mp_info_eof
                    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    process_mp_info_eof(connection, target_date, sftp, start_time)

                # 잘못된 process_type이 입력되었을 경우
                else:
                    log_message(
                        f"Invalid process_type: {process_type}. No process executed."
                    )

            except Exception as e:
                log_message(f"An error occurred: {e}")
                raise

            finally:
                # 연결 종료
                if "sftp" in locals():
                    sftp.close()
                if "transport" in locals():
                    transport.close()

    except Exception as e:
        log_message(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
