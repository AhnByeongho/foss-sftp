import argparse
import paramiko
from sqlalchemy import create_engine

from utils import (
    delete_old_bcp_data,
    insert_fnd_list_data,
    insert_customer_account_data,
    insert_customer_fund_data,
    process_yesterday_return_data,
    process_mp_list,
    process_rebalcus,
    process_report,
    process_mp_info_eof,
    log_message,
)


# sftp 연결 정보
fossDev_sftp_config = {
    "host": "106.10.52.176",
    "port": 2020,
    "user": "fossDev",
    "password": "qBi-nav-dev_2)0$"
}

foss_sftp_config = {
    "host": "106.10.52.176",
    "port": 2020,
    "user": "foss",
    "password": "qBi-nav_2)0$"
}


# MSSQL 연결 정보
local_db_config = {
    "server": "localhost",
    "database": "QBIS_RAB_LOCAL",
    "username": "SA",
    "password": "3170118A!",
}

test_db_config = {
    "server": "192.168.5.239",
    "database": "QBIS_RAB_TEST",
    "username": "qbrabtest",
    "password": "znjxjqorrabtest11!!",
}

dev_db_config = {
    "server": "192.168.5.239",
    "database": "QBIS_RAB_DEV",
    "username": "qbrabdev",
    "password": "znjxjqorrabdev11!!",
}

db_config = {
    "server": "192.168.5.239",
    "database": "QBIS_RAB",
    "username": "qbrab",
    "password": "znjxjqorrab11!!",
}


# TODO: 운영에 batch 돌릴 때는 수정해야함
def get_sftp_connection(process_type):
    if process_type in ["RECEIVE_UNIVERSE", "RECEIVE_ACCOUNT", "RECEIVE_CUSTMERFND"]:
        config = foss_sftp_config
    else:
        config = fossDev_sftp_config

    transport = paramiko.Transport((config["host"], config["port"]))
    transport.connect(username=config["user"], password=config["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def get_sqlalchemy_connection(db_config):
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
    args = parser.parse_args()

    target_date = args.target_date
    process_type = args.process_type

    try:
        # SFTP 연결
        sftp, transport = get_sftp_connection(process_type)

        # MSSQL 연결 TODO: 운영에 서버에 올릴 때는 수정해야함
        engine = get_sqlalchemy_connection(dev_db_config)

        # 엔진에서 연결 생성
        with engine.connect() as connection:

            # foss_data directory 접근
            sftp.chdir("foss_data")

            # TODO: batch_spid, running_key
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
                ap_acc_info = file_contents.get("ap_acc_info")  # TBL_FOSS_CUSTOMERACCOUNT
                ap_fnd_info = file_contents.get("ap_fnd_info")  # TBL_FOSS_CUSTOMERFUND

                # ------------------ 1개월 전 데이터 삭제 (TBL_FOSS_BCPDATA) ------------------ #
                if process_type == "DELETE_OLDDATA":
                    delete_old_bcp_data(connection)

                # ------------------------------ 유니버스 수신 ------------------------------- #
                elif process_type == "RECEIVE_UNIVERSE":  # TBL_FOSS_UNIVERSE
                    if fnd_list:
                        insert_fnd_list_data(connection, fnd_list, target_date)
                    else:
                        log_message(f"No data fnd_list found for {target_date}.")

                # ---------------------------- 고객 계좌 정보 수신 ---------------------------- #
                elif process_type == "RECEIVE_ACCOUNT":  # TBL_FOSS_CUSTOMERACCOUNT
                    if ap_acc_info:
                        insert_customer_account_data(connection, ap_acc_info, target_date)
                    else:
                        log_message(f"No data ap_acc_info found for {target_date}.")

                # --------------------------- 고객 보유펀드 정보 수신 --------------------------- #
                elif process_type == "RECEIVE_CUSTMERFND":  # TBL_FOSS_CUSTOMERFUND
                    if ap_fnd_info:
                        insert_customer_fund_data(connection, ap_fnd_info, target_date)
                    else:
                        log_message(f"No data ap_fnd_info found for {target_date}.")

                # ----------- 전일 수익률 송신 처리(최근 영업일에 수익률 자료가 있을때만 생성) ------------ #
                elif process_type == "SEND_MPRATE":     # mp_info
                    process_yesterday_return_data(connection, target_date, sftp)

                # ----------------------------- MP 리스트 송신 처리 ---------------------------- #
                elif process_type == "SEND_MPLIST":     # mp_fnd_info
                    process_mp_list(connection, target_date, sftp)

                # --------------------------- 리밸런싱 고객자료 송신 처리 ------------------------- #
                elif process_type == "SEND_REBALCUS":   # ap_reval_yn
                    # 리밸런싱 송신 처리
                    process_rebalcus(connection, target_date, sftp)

                    # 강제 리밸런싱일자 설정
                    # forced_rebal_dates = ["20231201", "20241201"]
                    # process_rebalcus(connection, target_date, sftp, forced_rebal_dates=forced_rebal_dates)

                    # 수동 리밸런싱 (특정 일자에 해당 고객만 강제 리밸런싱)
                    # manual_customer_ids = ["10083", "10096", "10113"]
                    # manual_rebal_yn = "Y"
                    # process_rebalcus(connection, target_date, sftp, manual_customer_ids=manual_customer_ids, manual_rebal_yn=manual_rebal_yn)

                # ------------------------------ 리포트 송신 처리 ------------------------------- #
                elif process_type == "SEND_REPORT":     # report
                    process_report(connection, target_date, sftp)

                # ------------------------- MP_INFO_EOF 빈파일 송신 처리 ------------------------ #
                elif process_type == "SEND_MP_INFO_EOF":    # mp_info_eof
                    process_mp_info_eof(target_date, sftp)

                # 잘못된 process_type이 입력되었을 경우
                else:
                    log_message(f"Invalid process_type: {process_type}. No process executed.")

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
