import pandas as pd
import numpy as np
import holidays
import os
import csv
from sqlalchemy import create_engine, text
from io import StringIO
from datetime import datetime, timedelta


def get_sqlalchemy_connection(db_config):
    """
    Returns a SQLAlchemy engine for SQL Server connection.
    """
    connection_url = (
        f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@"
        f"{db_config['server']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(connection_url)


def delete_old_bcp_data(engine):
    """
    Deletes data older than 1 month from the TBL_FOSS_BCPDATA table.
    """
    try:
        with engine.connect() as connection:
            delete_query = text("""
            DELETE FROM TBL_FOSS_BCPDATA
            WHERE LEFT(indate, 8) < FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMMdd')
            """)
            connection.execute(delete_query)
            print("Old BCP data older than 1 month deleted successfully.")
    except Exception as e:
        print(f"An error occurred while deleting old BCP data: {e}")


def insert_fnd_list_data(engine, fnd_list, target_date):
    """
    Inserts data from fnd_list into TBL_FOSS_UNIVERSE after removing duplicates.

    :param engine: SQLAlchemy engine
    :param fnd_list: String content of the fund list
    :param target_date: Date for which the data is being inserted
    """
    try:
        # 엔진에서 연결 생성
        with engine.connect() as connection:
            with connection.begin():
                # 해당 날짜 데이터 중복 여부 확인
                check_query = """
                SELECT COUNT(*) FROM TBL_FOSS_UNIVERSE WHERE trddate = :target_date
                """
                result = connection.execute(
                    text(check_query), {"target_date": target_date}
                ).scalar()

                if result > 0:
                    print(
                        f"Data(fnd_list) for {target_date} already exists. Skipping insertion."
                    )
                    return

                # fnd_list 데이터 파싱 및 중복 제거
                csv_reader = csv.reader(StringIO(fnd_list), delimiter=";")
                unique_rows = set()  # 중복 제거를 위한 집합
                for row in csv_reader:
                    if len(row) == 12:  # 데이터 유효성 검사
                        unique_rows.add(
                            tuple(row)
                        )  # 리스트를 튜플로 변환 후 집합에 추가

                # 삽입 쿼리
                insert_query = """
                INSERT INTO TBL_FOSS_UNIVERSE (
                    trddate, fund_cd, foss_fund_cd, fund_nm, fund_cd_s, tradeyn,
                    class_gb, risk_grade, investgb, co_cd, co_nm, total_cnt, regdate
                )
                VALUES (:trddate, :fund_cd, :foss_fund_cd, :fund_nm, :fund_cd_s, :tradeyn,
                        :class_gb, :risk_grade, :investgb, :co_cd, :co_nm, :total_cnt, GETDATE())
                """
                for row in unique_rows:
                    try:
                        connection.execute(
                            text(insert_query),
                            {
                                "trddate": target_date,
                                "fund_cd": row[1].strip(),
                                "foss_fund_cd": row[2].strip(),
                                "fund_nm": row[3].strip(),
                                "fund_cd_s": row[4].strip(),
                                "tradeyn": row[5].strip(),
                                "class_gb": row[6].strip(),
                                "risk_grade": row[7].strip(),
                                "investgb": row[8].strip(),
                                "co_cd": row[9].strip(),
                                "co_nm": row[10].strip(),
                                "total_cnt": int(row[11].strip()),
                            },
                        )
                    except ValueError as ve:
                        print(f"Data parsing error for row: {row} | Error: {ve}")
                        continue

            print("Data(fnd_list) inserted successfully with duplicates removed.")

    except Exception as e:
        print(f"An error occurred while inserting data(fnd_list): {e}")


def insert_customer_account_data(engine, ap_acc_info, target_date):
    """
    Inserts data from ap_acc_info into TBL_FOSS_CUSTOMERACCOUNT after removing duplicates.

    :param engine: SQLAlchemy engine
    :param ap_acc_info: String content of the customer account information
    :param target_date: Date for which the data is being inserted
    """
    try:
        # 엔진에서 연결 생성
        with engine.connect() as connection:
            with connection.begin():
                # 중복 데이터 확인
                check_query = """
                SELECT COUNT(*) AS cnt FROM TBL_FOSS_CUSTOMERACCOUNT WHERE trddate = :target_date
                """
                count = connection.execute(
                    text(check_query), {"target_date": target_date}
                ).scalar()

                if count > 0:
                    print(
                        f"Data(ap_acc_info) for {target_date} already exists. Skipping insertion."
                    )
                    return

                # 데이터 파싱 및 중복 제거
                csv_reader = csv.reader(StringIO(ap_acc_info), delimiter=";")
                unique_rows = set()  # 중복 제거를 위한 집합
                for row in csv_reader:
                    if len(row) == 8:  # 데이터 유효성 검사
                        unique_rows.add(
                            tuple(row)
                        )  # 리스트를 튜플로 변환 후 집합에 추가

                # 삽입 쿼리
                insert_query = """
                INSERT INTO TBL_FOSS_CUSTOMERACCOUNT (
                    trddate, customer_id, investgb, risk_grade, invest_principal,
                    totalappraisal_price, revenue_price, order_status, deposit_price, regdate
                ) VALUES (
                    :trddate, :customer_id, :investgb, :risk_grade, :invest_principal,
                    :totalappraisal_price, :revenue_price, :order_status, :deposit_price, GETDATE()
                )
                """
                for row in unique_rows:
                    try:
                        # 데이터 삽입
                        connection.execute(
                            text(insert_query),
                            {
                                "trddate": target_date,
                                "customer_id": row[0].strip(),
                                "investgb": row[1].strip(),
                                "risk_grade": row[2].strip(),
                                "invest_principal": int(row[3].strip()),
                                "totalappraisal_price": int(row[4].strip()),
                                "revenue_price": int(row[5].strip()),
                                "order_status": row[6].strip(),
                                "deposit_price": int(row[7].strip()),
                            },
                        )
                    except ValueError as ve:
                        print(
                            f"Data(ap_acc_info) conversion error for row: {row} | Error: {ve}"
                        )
                        continue

            print("Data(ap_acc_info) inserted successfully with duplicates removed.")

    except Exception as e:
        print(f"An error occurred while inserting data(ap_acc_info): {e}")


def insert_customer_fund_data(engine, ap_fnd_info, target_date):
    """
    Inserts data from ap_fnd_info into TBL_FOSS_CUSTOMERFUND after removing duplicates.

    :param engine: SQLAlchemy engine
    :param ap_fnd_info: String content of the customer fund information
    :param target_date: Date for which the data is being inserted
    """
    try:
        # 엔진에서 연결 생성
        with engine.connect() as connection:
            with connection.begin():
                # 중복 데이터 확인
                check_query = """
                SELECT COUNT(*) AS cnt FROM TBL_FOSS_CUSTOMERFUND WHERE trddate = :target_date
                """
                count = connection.execute(
                    text(check_query), {"target_date": target_date}
                ).scalar()

                if count > 0:
                    print(
                        f"Data(ap_fnd_info) for {target_date} already exists. Skipping insertion."
                    )
                    return

                # 데이터 파싱 및 중복 제거
                csv_reader = csv.reader(StringIO(ap_fnd_info), delimiter=";")
                unique_rows = set()  # 중복 제거를 위한 집합
                for row in csv_reader:
                    if len(row) == 5:  # 데이터 유효성 검사
                        unique_rows.add(
                            tuple(row)
                        )  # 리스트를 튜플로 변환 후 집합에 추가

                # 삽입 쿼리
                insert_query = """
                INSERT INTO TBL_FOSS_CUSTOMERFUND (
                    trddate, customer_id, fund_cd, invest_principal, appraisal_price, revenue_price, regdate
                ) VALUES (
                    :trddate, :customer_id, :fund_cd, :invest_principal, :appraisal_price, :revenue_price, GETDATE()
                )
                """
                for row in unique_rows:
                    try:
                        # 데이터 삽입
                        connection.execute(
                            text(insert_query),
                            {
                                "trddate": target_date,
                                "customer_id": row[0].strip(),
                                "fund_cd": row[1].strip(),
                                "invest_principal": int(row[2].strip()),
                                "appraisal_price": int(row[3].strip()),
                                "revenue_price": int(row[4].strip()),
                            },
                        )
                    except ValueError as ve:
                        print(
                            f"Data(ap_fnd_info) conversion error for row: {row} | Error: {ve}"
                        )
                        continue

            print("Data(ap_fnd_info) inserted successfully with duplicates removed.")

    except Exception as e:
        print(f"An error occurred while inserting data(ap_fnd_info): {e}")


def process_yesterday_return_data(engine, target_date, sftp_client):
    """
    Processes yesterday's return data and validates before proceeding with SFTP transmission.

    - Fetches the most recent business date (fund base date).
    - Validates data consistency between TBL_RESULT_RETURN and TBL_RESULT_MPLIST.
    - Calculates risk grades, return terms, and associated performance metrics.
    - Generates a final CSV file containing the data.
    - Inserts data into TBL_FOSS_BCPDATA table.
    - Uploads the CSV file to the SFTP server.

    :param engine: SQLAlchemy engine
    :param target_date: Target date for processing
    :param sftp_client: SFTP client for file operations
    """
    try:
        # 현재 날짜와 기준 날짜 설정
        if not target_date:
            target_date = datetime.now().strftime("%Y%m%d")

        # 파일 이름 설정
        sSetFile = f"mp_info.{target_date}"

        with engine.connect() as connection:
            with connection.begin():
                # 최근 영업일 계산
                sFndDate = get_recent_business_date(target_date)

                if not sFndDate:
                    raise ValueError("No valid fund base date found.")

                # 생성해야 되는 데이터 확인
                query_result_return = """
                SELECT COUNT(auth_id)
                FROM TBL_RESULT_RETURN
                WHERE auth_id = :auth_id AND trddate = :trddate
                """
                count_result_return = connection.execute(
                    text(query_result_return), {"auth_id": "foss", "trddate": sFndDate}
                ).scalar()

                query_result_mplist = """
                SELECT COUNT(S1.port_cd)
                FROM (
                    SELECT port_cd
                    FROM TBL_RESULT_MPLIST
                    WHERE auth_id = :auth_id
                        AND rebal_date = (
                            SELECT MAX(rebal_date) FROM TBL_RESULT_MPLIST WHERE auth_id = :auth_id
                        )
                    GROUP BY port_cd
                ) AS S1
                """
                count_port_cd = connection.execute(
                    text(query_result_mplist), {"auth_id": "foss"}
                ).scalar()

                if count_result_return != count_port_cd:
                    print(
                        "Data mismatch between TBL_RESULT_RETURN and TBL_RESULT_MPLIST. Process stopped."
                    )
                    return  # 프로세스 중단

                # TMP_PERFORMANCE 데이터프레임 생성
                TMP_PERFORMANCE = get_tmp_performance(connection, "foss", sFndDate)

                terms_to_merge = ["1m", "3m", "6m", "1y", "all"]
                merged = merge_terms(TMP_PERFORMANCE, terms_to_merge)
                merged = add_expected_return_and_volatility(merged)
                merged = merged.sort_values(by=["prd_gb", "risk_grade"]).reset_index(drop=True)
                merged = create_return_lst_column(merged, sFndDate)
                merged["idx"] = merged.index + 1

                # Final Dataframe
                final_df = prepare_final_df(merged, sSetFile)

                # TBL_FOSS_BCPDATA 테이블에 데이터 삽입
                insert_bcpdata(connection, final_df)
                print(
                    "Data(mp_info) has been inserted into the TBL_FOSS_BCPDATA table."
                )

                # CSV 파일 저장
                local_file_path = (
                    f"/Users/mac/Downloads/{sSetFile}.csv"  # 로컬 경로 설정
                )
                final_df[["lst"]].to_csv(
                    local_file_path, index=False, header=False, encoding="utf-8"
                )

        # SFTP 경로 및 파일 설정
        remote_path = f"../robo_data/{sSetFile}"  # 원격 파일 경로
        local_path = local_file_path  # 로컬에서 저장한 파일 경로

        # SFTP 업로드
        sftp_client.put(local_path, remote_path)
        print(f"File successfully uploaded to SFTP server: {remote_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        # 임시 CSV 파일 삭제
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
        except Exception as e:
            print(f"Error occurred while deleting the temporary CSV file: {e}")


def process_mp_list(engine, target_date, sftp_client):
    """
    Processes the MP List data, generates a CSV file, inserts data into the TBL_FOSS_BCPDATA table,
    and uploads the file to the SFTP server.

    - Fetches the MP List data from TBL_RESULT_MPLIST and TBL_FOSS_UNIVERSE.
    - Generates a formatted list (`lst`) containing port_cd, product information, and weights.
    - Inserts the processed data into the TBL_FOSS_BCPDATA table.
    - Generates a CSV file for transmission.
    - Uploads the CSV file to the SFTP server.

    :param engine: SQLAlchemy engine instance used for database operations.
    :param target_date: The target date for processing.
    :param sftp_client: Configured SFTP client for file transmission.
    """
    try:
        # 파일 이름 설정
        sSetFile = f"mp_fnd_info.{target_date}"

        with engine.connect() as connection:
            with connection.begin():
                # MP List 데이터 조회
                raw_df = fetch_mp_list_data(connection, target_date, "foss")

                # 데이터 전처리
                raw_df = preprocess_mp_list_data(raw_df)

                # Final Dataframe
                final_df = prepare_final_df(raw_df, sSetFile)

                # TBL_FOSS_BCPDATA 테이블에 데이터 삽입
                insert_bcpdata(connection, final_df)
                print(
                    "Data(mp_fnd_info) has been inserted into the TBL_FOSS_BCPDATA table."
                )

                # CSV 파일 저장
                local_file_path = (
                    f"/Users/mac/Downloads/{sSetFile}.csv"  # 로컬 경로 설정
                )
                final_df[["lst"]].to_csv(
                    local_file_path, index=False, header=False, encoding="utf-8"
                )

        # SFTP 경로 및 파일 설정
        remote_path = f"../robo_data/{sSetFile}"  # 원격 파일 경로
        local_path = local_file_path  # 로컬에서 저장한 파일 경로

        # SFTP 업로드
        sftp_client.put(local_path, remote_path)
        print(f"File successfully uploaded to SFTP server: {remote_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        # 임시 CSV 파일 삭제
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
        except Exception as e:
            print(f"Error occurred while deleting the temporary CSV file: {e}")


def process_rebalcus(
    engine,
    target_date,
    sftp_client,
    manual_customer_ids=None,
    manual_rebal_yn=None,
    forced_rebal_dates=None,
):
    """
    Processes the Rebalancing Customer data, generates a CSV file, inserts data into the TBL_FOSS_BCPDATA table,
    and uploads the file to the SFTP server.

    - Determines if the current date is a rebalancing date for pension and general investment products.
    - Calculates the next rebalancing date for both pension and general investment products.
    - Fetches customer rebalancing data from TBL_FOSS_CUSTOMERACCOUNT based on the calculated dates.
    - Inserts the processed data into the TBL_FOSS_BCPDATA table.
    - Handles manual rebalancing signals if specific customer IDs are provided.
    - Generates a CSV file for transmission.
    - Uploads the CSV file to the SFTP server.

    :param engine: SQLAlchemy engine instance used for database operations.
    :param target_date: The target date for processing.
    :param sftp_client: Configured SFTP client for file transmission.
    :param manual_customer_ids: (Optional) List of customer IDs for manual rebalancing.
    :param manual_rebal_yn: (Optional) Manual rebalancing signal ('Y' or 'N') for specified customers.
    """
    try:
        # 파일 이름 설정
        sSetFile = f"ap_reval_yn.{target_date}"

        with engine.connect() as connection:
            with connection.begin():
                # 리밸런싱 여부 확인 (f12: 연금, f11: 일반)
                sRebalDayYN = check_rebalancing(connection, target_date, "foss", "f12")     # 연금(f12) 리밸런싱 여부
                sRebalDayYN2 = check_rebalancing(connection, target_date, "foss", "f11")     # 연금(f11) 리밸런싱 여부

                i_opent_day = 3  # 영업일

                # 연금(f12), 일반(f11) 다음 리밸런싱 날짜 계산
                next_rebal_date = get_next_rebalancing_date(connection, target_date, i_opent_day)

                # 강제 리밸런싱 날짜 (Disable된 상태, 필요한 경우만 활성화)
                current_date = datetime.now().strftime("%Y%m%d")
                if forced_rebal_dates is not None:
                    if current_date in forced_rebal_dates:
                        sRebalDayYN = "Y"
                        sRebalDayYN2 = "Y"

                # TBL_FOSS_CUSTOMERACCOUNT에서 데이터 조회 및 처리
                pension_data = fetch_rebalcus_data(connection, target_date, "77", sRebalDayYN, next_rebal_date, sSetFile)   # 연금(f12) 데이터 조회
                general_data = fetch_rebalcus_data(connection, target_date, "61", sRebalDayYN2, next_rebal_date, sSetFile)  # 일반(f11) 데이터 조회

                # Final Dataframe
                final_rebalcus_data = prepare_final_rebalcus_df([pension_data, general_data])

                # TBL_FOSS_BCPDATA 테이블에 데이터 삽입
                insert_bcpdata(connection, final_rebalcus_data)
                print(
                    "Data(ap_reval_yn) has been inserted into the TBL_FOSS_BCPDATA table."
                )

                # 수동 리벨런싱 (특정 일자에 해당 고객만 강제 리밸런싱)
                if manual_customer_ids is not None and manual_rebal_yn is not None:
                    update_manual_rebalancing(connection, final_rebalcus_data, manual_customer_ids, manual_rebal_yn, target_date, sSetFile)

                # CSV 파일 저장
                local_file_path = (
                    f"/Users/mac/Downloads/{sSetFile}.csv"  # 로컬 경로 설정
                )
                final_rebalcus_data[["lst"]].to_csv(
                    local_file_path, index=False, header=False, encoding="utf-8"
                )

        # SFTP 경로 및 파일 설정
        remote_path = f"../robo_data/{sSetFile}"  # 원격 파일 경로
        local_path = local_file_path  # 로컬에서 저장한 파일 경로

        # SFTP 업로드
        sftp_client.put(local_path, remote_path)
        print(f"File successfully uploaded to SFTP server: {remote_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        # 임시 CSV 파일 삭제
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
        except Exception as e:
            print(f"Error occurred while deleting the temporary CSV file: {e}")


def process_report(engine, target_date, sftp_client):
    """
    Processes report data for the specified target date, generates a CSV file,
    inserts the data into the TBL_FOSS_BCPDATA table, and uploads the file to the SFTP server.

    - Checks if there is any data in TBL_FOSS_REPORT for the target date.
    - If data is available, it fetches the latest report data (up to the target date).
    - Cleans the `performance_t` and `performance_c` fields by applying string replacements.
    - Generates the `lst` field for each row and inserts the processed data into TBL_FOSS_BCPDATA.
    - Creates a CSV file containing the `lst` data for transmission.
    - Uploads the generated CSV file to the SFTP server.

    :param engine: SQLAlchemy engine instance used for database operations.
    :param target_date: The target date for processing the report data (format: YYYYMMDD).
    :param sftp_client: Configured SFTP client for file transmission.
    """
    try:
        # 파일 이름 설정
        sSetFile = f"report.{target_date}"

        with engine.connect() as connection:
            with connection.begin():
                # TBL_FOSS_REPORT에서 오늘 날짜와 동일한 trddate가 있는지 확인
                check_query = text("""
                    SELECT COUNT(trddate) 
                    FROM TBL_FOSS_REPORT 
                    WHERE trddate = :target_date
                """)
                result = connection.execute(
                    check_query, {"target_date": target_date}
                ).scalar()

                if result >= 1:
                    # trddate가 오늘 날짜인 데이터를 가져와서 TBL_FOSS_BCPDATA에 삽입
                    select_query = text("""
                        SELECT trddate, performance_t, performance_c
                        FROM TBL_FOSS_REPORT
                        WHERE trddate = (
                            SELECT MAX(trddate) 
                            FROM TBL_FOSS_REPORT 
                            WHERE trddate <= :target_date
                        )
                    """)
                    df = pd.read_sql(
                        select_query, connection, params={"target_date": target_date}
                    )

                    # 문자열 전처리: performance_t와 performance_c
                    def clean_string(value):
                        value = value.replace('"', "&quot;")  # "를 &quot;로 변경
                        value = value.replace("\r", "\n")  # CHAR(13) -> \n
                        value = value.replace("\n", "")  # CHAR(10)을 없애기
                        value = value.replace(";", "")  # ;을 없애기
                        return value

                    # BCP 데이터 삽입 준비
                    insert_data = []
                    for idx, row in df.iterrows():
                        performance_t = clean_string(row["performance_t"])
                        performance_c = clean_string(row["performance_c"])

                        lst = f"{row['trddate']};{performance_t};{performance_c};"
                        insert_data.append(
                            {
                                "indate": datetime.now().strftime("%Y%m%d%H%M%S"),
                                "send_filename": sSetFile,
                                "idx": idx + 1,  # ROW_NUMBER() 대체
                                "lst": lst,
                            }
                        )
                    # insert_data를 DataFrame으로 변환
                    insert_df = pd.DataFrame(insert_data)

                    # TBL_FOSS_BCPDATA에 삽입
                    insert_query = text("""
                        INSERT INTO TBL_FOSS_BCPDATA (indate, send_filename, idx, lst)
                        VALUES (:indate, :send_filename, :idx, :lst)
                    """)
                    connection.execute(insert_query, insert_data)

                    print(
                        f"Report data for {target_date} has been processed and inserted."
                    )

                    # CSV 파일 저장
                    local_file_path = (
                        f"/Users/mac/Downloads/{sSetFile}.csv"  # 로컬 경로 설정
                    )
                    insert_df[["lst"]].to_csv(
                        local_file_path, index=False, header=False, encoding="utf-8"
                    )

                else:
                    print(f"No report data found for {target_date}.")

        # SFTP 경로 및 파일 설정
        remote_path = f"../robo_data/{sSetFile}"  # 원격 파일 경로
        local_path = local_file_path  # 로컬에서 저장한 파일 경로

        # SFTP 업로드
        sftp_client.put(local_path, remote_path)
        print(f"File successfully uploaded to SFTP server: {remote_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        # 임시 CSV 파일 삭제
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
        except Exception as e:
            print(f"Error occurred while deleting the temporary CSV file: {e}")


def process_mp_info_eof(target_date, sftp_client):
    """
    Generates an EOF (End of File) for mp_info and uploads it to the SFTP server.

    - Creates an empty CSV file with the specified name.
    - Uploads the file to the SFTP server.

    :param engine: SQLAlchemy engine instance used for database operations.
    :param target_date: The target date for the EOF file.
    :param sftp_client: Configured SFTP client for file transmission.
    """
    try:
        # 파일 이름 설정
        sSetFile = f"mp_info_eof.{target_date}"
        local_file_path = f"/Users/mac/Downloads/{sSetFile}.csv"  # 로컬 경로 설정

        # 빈 CSV 파일 생성
        with open(local_file_path, "w", encoding="utf-8") as file:
            file.write("")  # 빈 파일 생성

        print(f"Empty EOF file created at: {local_file_path}")

        # SFTP 경로 및 파일 설정
        remote_path = f"../robo_data/{sSetFile}"  # 원격 파일 경로

        # SFTP 업로드
        sftp_client.put(local_file_path, remote_path)
        print(f"Empty EOF file successfully uploaded to SFTP server: {remote_path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        # 임시 CSV 파일 삭제
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
        except Exception as e:
            print(f"Error occurred while deleting the temporary CSV file: {e}")


def get_recent_business_date(target_date):
    kr_holidays = holidays.KR()
    target_date = datetime.strptime(target_date, "%Y%m%d")

    # 오늘이 영업일인지 확인
    is_today_business_day = target_date.weekday() < 5 and target_date not in kr_holidays

    # 오늘이 영업일이면 오늘을 제외하고 1영업일 전을 찾기
    if is_today_business_day:
        recent_business_date = target_date - timedelta(days=1)
        while recent_business_date.weekday() >= 5 or recent_business_date in kr_holidays:
            recent_business_date -= timedelta(days=1)
        return recent_business_date.strftime("%Y%m%d")

    # 오늘이 공휴일이면 2영업일 전을 찾기
    else:
        recent_business_date = target_date - timedelta(days=1)
        count = 0
        while count < 2:
            if recent_business_date.weekday() < 5 and recent_business_date not in kr_holidays:
                count += 1
            if count < 2:
                recent_business_date -= timedelta(days=1)
    recent_business_date = recent_business_date.strftime("%Y%m%d")

    return recent_business_date


def get_tmp_riskgrade(connection, auth_id):
    query = """
    SELECT 
        auth_id, 
        port_cd, 
        RIGHT(port_cd, 1) AS risk_grade,
        prd_gb
    FROM TBL_RESULT_MPLIST
    WHERE auth_id = :auth_id
    GROUP BY auth_id, port_cd, prd_gb
    """

    return pd.read_sql(text(query), connection, params={"auth_id": auth_id})


def get_tmp_return(connection, sFndDate):
    terms = [
        ("1d", sFndDate, sFndDate),
        ("1m", (pd.to_datetime(sFndDate) - pd.DateOffset(months=1) + pd.DateOffset(days=1)).strftime("%Y%m%d"), sFndDate),
        ("3m", (pd.to_datetime(sFndDate) - pd.DateOffset(months=3) + pd.DateOffset(days=1)).strftime("%Y%m%d"), sFndDate),
        ("6m", (pd.to_datetime(sFndDate) - pd.DateOffset(months=6) + pd.DateOffset(days=1)).strftime("%Y%m%d"), sFndDate),
        ("1y", (pd.to_datetime(sFndDate) - pd.DateOffset(years=1) + pd.DateOffset(days=1)).strftime("%Y%m%d"), sFndDate),
        ("all", "20181203", sFndDate),
    ]

    valid_terms = [
        (term, start, end)
        for term, start, end in terms
        if pd.to_datetime(start, format="%Y%m%d", errors="coerce") >= pd.to_datetime("20181204", format="%Y%m%d") or term == "all"
    ]

    term_df = pd.DataFrame(valid_terms, columns=["term", "start_dt", "end_dt"])

    query = """
    SELECT auth_id, port_cd, trddate, rtn_1d
    FROM TBL_RESULT_RETURN
    """
    result_return = pd.read_sql(text(query), connection)

    all_returns = []
    for _, row in term_df.iterrows():
        filtered = result_return[
            (result_return["trddate"] >= row["start_dt"]) &
            (result_return["trddate"] <= row["end_dt"])
        ].copy()
        filtered["term"] = row["term"]
        filtered["start_dt"] = row["start_dt"]
        filtered["end_dt"] = row["end_dt"]
        all_returns.append(filtered)

    return pd.concat(all_returns, ignore_index=True)


def calculate_performance(tmp_riskgrade, tmp_return):
    tmp_merged = pd.merge(
        tmp_riskgrade,
        tmp_return,
        on=["auth_id", "port_cd"],
        how="left",
        suffixes=("", "_extra"),
    )

    tmp_return = tmp_merged[
        [
            "auth_id",
            "port_cd",
            "risk_grade",
            "prd_gb",
            "term",
            "start_dt",
            "end_dt",
            "trddate",
            "rtn_1d",
        ]
    ]

    tmp_performance = tmp_return.groupby(["auth_id", "term", "risk_grade", "prd_gb"], as_index=False).agg(
        total_rt=("rtn_1d", lambda x: (x + 1).prod() * 100 - 100)
    )
    tmp_performance["total_rt"] = tmp_performance["total_rt"].round(2)

    return tmp_performance[["auth_id", "term", "risk_grade", "prd_gb", "total_rt"]]


def get_tmp_performance(connection, auth_id, sFndDate):
    tmp_riskgrade = get_tmp_riskgrade(connection, auth_id)
    tmp_return = get_tmp_return(connection, sFndDate)
    tmp_performance = calculate_performance(tmp_riskgrade, tmp_return)

    return tmp_performance


def merge_terms(tmp_performance, terms):
    merged = tmp_performance[tmp_performance["term"] == "1d"].copy()
    for term in terms:
        term_df = tmp_performance[tmp_performance["term"] == term].copy()
        merged = merged.merge(
            term_df,
            on=["risk_grade", "prd_gb"],
            suffixes=("", f"_{term}"),
            how="left",
        )

    return merged


expected_return_map = {
    "1": {"f12": "9.10", "f11": "9.04"},
    "2": {"f12": "12.40", "f11": "12.53"},
    "3": {"f12": "16.02", "f11": "16.64"},
    "4": {"f12": "19.14", "f11": "19.80"},
    "5": {"f12": "21.15", "f11": "22.99"},
}

volatility_map = {
    "1": {"f12": "3.20", "f11": "3.10"},
    "2": {"f12": "4.46", "f11": "4.57"},
    "3": {"f12": "6.68", "f11": "6.87"},
    "4": {"f12": "8.60", "f11": "9.21"},
    "5": {"f12": "10.90", "f11": "11.51"},
}

def add_expected_return_and_volatility(df):
    df["expected_return"] = df.apply(lambda row: expected_return_map.get(row["risk_grade"], {}).get(row["prd_gb"], ""), axis=1)
    df["volatility"] = df.apply(lambda row: volatility_map.get(row["risk_grade"], {}).get(row["prd_gb"], ""), axis=1)
    return df


def create_return_lst_column(df, sFndDate):
    df["lst"] = df.apply(
        lambda row: (
            f"{sFndDate};"
            f"{row['risk_grade']};"
            f"{ {'f12': '77', 'f11': '61'}.get(row['prd_gb'], '') };"
            f"{row['total_rt']};"
            f"{row['total_rt_3m']};"
            f"{row['total_rt_6m']};"
            f"{row['total_rt_1y']};"
            f"{row['total_rt_all']};"
            f"{row['expected_return']};"
            f"{row['volatility']};"
            f"{row['total_rt_1m']};"
        ),
        axis=1
    )

    return df


def fetch_mp_list_data(connection, target_date, auth_id):
    query = """
    SELECT 
        S1.port_cd,
        S1.prd_gb,
        S1.prd_cd,
        S1.prd_weight,
        S2.fund_nm
    FROM TBL_RESULT_MPLIST S1
    LEFT OUTER JOIN TBL_FOSS_UNIVERSE S2 
        ON S2.fund_cd = S1.prd_cd 
        AND S2.trddate = (SELECT MAX(trddate) FROM TBL_FOSS_UNIVERSE)
    INNER JOIN (
        SELECT port_cd, MAX(rebal_date) AS rebal_date 
        FROM TBL_RESULT_MPLIST 
        WHERE auth_id = :auth_id AND rebal_date <= :target_date 
        GROUP BY port_cd
    ) S3 
        ON S3.port_cd = S1.port_cd AND S3.rebal_date = S1.rebal_date
    WHERE S1.auth_id = :auth_id
    ORDER BY S1.port_cd ASC
    """

    return pd.read_sql(
        text(query),
        connection,
        params={"target_date": target_date, "auth_id": auth_id},
    )


def preprocess_mp_list_data(raw_df):
    # fund_nm 길이 100 초과 시 자르기
    raw_df["fund_nm"] = raw_df["fund_nm"].fillna("")
    raw_df["fund_nm"] = raw_df["fund_nm"].apply(
        lambda x: x[:100] if len(x) > 100 else x
    )

    # port_cd의 마지막 문자 추출
    raw_df["port_cd_last_char"] = raw_df["port_cd"].str[-1]

    # prd_gb 매핑
    prd_gb_map = {"f12": "77", "f11": "61"}
    raw_df["prd_gb_mapped"] = raw_df["prd_gb"].map(prd_gb_map)

    # lst 컬럼 생성
    raw_df["lst"] = (
        raw_df["port_cd_last_char"]
        + ";"
        + raw_df["prd_gb_mapped"]
        + ";"
        + raw_df["prd_cd"]
        + ";"
        + raw_df["fund_nm"]
        + ";"
        + (raw_df["prd_weight"] / 100).round(2).astype(str)
        + ";"
    )

    # ROW_NUMBER (idx) 추가
    raw_df["idx"] = raw_df.index + 1

    return raw_df


def check_rebalancing(connection, target_date, auth_id, prd_gb):
    query = """
    SELECT 
        CASE WHEN COUNT(*) = 0 THEN 'N' ELSE 'Y' END AS rebal_day_yn
    FROM TBL_RESULT_MPLIST 
    WHERE auth_id = :auth_id 
        AND rebal_date = :target_date 
        AND prd_gb = :prd_gb
    """
    result = connection.execute(
        text(query),
        {"auth_id": auth_id, "target_date": target_date, "prd_gb": prd_gb},
    ).scalar()

    return result


def get_next_rebalancing_date(connection, target_date, i_opent_day):
    query = """
        SELECT MIN(trddate) AS next_rebal_date
        FROM (
            SELECT 
                trddate,
                ROW_NUMBER() OVER (PARTITION BY LEFT(trddate, 6) ORDER BY trddate ASC) AS MonthCnt
            FROM TBL_HOLIDAY
            WHERE LEFT(trddate, 6) >= LEFT(:target_date, 6)
                AND holiday_yn = 'N'
                AND SUBSTRING(trddate, 5, 2) IN ('01', '04', '07', '10')
        ) S1
        WHERE S1.trddate >= :target_date
            AND S1.MonthCnt = :i_opent_day
    """
    result = connection.execute(
        text(query),
        {"target_date": target_date, "i_opent_day": i_opent_day},
    ).fetchone()
    
    return result[0] if result else ""


def fetch_rebalcus_data(connection, target_date, investgb, rebal_day_yn, next_rebal_date, sSetFile):
    query = """
        SELECT 
            :indate AS indate,
            :send_filename AS send_filename,
            ROW_NUMBER() OVER (ORDER BY customer_id ASC) AS idx,
            customer_id + ';' + 
            CASE 
                WHEN :rebal_day_yn = 'N' THEN 'N'
                ELSE 
                    CASE 
                        WHEN :rebal_day_yn = 'Y' AND order_status IN ('Y', 'Y1', 'Y3') THEN 'Y'
                        ELSE 'N'
                    END
            END + ';' + 
            :next_rebal_date + ';' AS lst
        FROM TBL_FOSS_CUSTOMERACCOUNT
        WHERE trddate = :target_date
            AND investgb = :investgb
    """
    return pd.read_sql(
        text(query),
        connection,
        params={
            "indate": datetime.now().strftime("%Y%m%d%H%M%S"),
            "send_filename": sSetFile,
            "rebal_day_yn": rebal_day_yn,
            "next_rebal_date": next_rebal_date,
            "target_date": target_date,
            "investgb": investgb,
        },
    )


def update_manual_rebalancing(connection, final_rebalcus_data, manual_customer_ids, manual_rebal_yn, target_date, sSetFile):
    for customer_id in manual_customer_ids:
        # 업데이트할 lst 값 생성
        updated_lst_value = f"{customer_id};{manual_rebal_yn};{target_date};"

        # 데이터프레임 내 업데이트
        final_rebalcus_data.loc[
            final_rebalcus_data["lst"].str.startswith(f"{customer_id};"),
            "lst",
        ] = updated_lst_value

        # TBL_FOSS_BCPDATA 테이블에 업데이트 실행
        update_query = """
        UPDATE TBL_FOSS_BCPDATA
        SET lst = :updated_lst
        WHERE send_filename = :send_filename
            AND indate = :indate
            AND lst LIKE :customer_id_prefix
        """
        connection.execute(
            text(update_query),
            {
                "updated_lst": updated_lst_value,
                "send_filename": sSetFile,
                "indate": final_rebalcus_data["indate"].iloc[0],
                "customer_id_prefix": f"{customer_id}%;",
            },
        )
    print(f"Manual rebalancing applied and updated in TBL_FOSS_BCPDATA for customers: {manual_customer_ids}")


def prepare_final_rebalcus_df(dataframes):
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df["idx"] = range(1, len(combined_df) + 1)

    return combined_df.sort_values(by="idx").reset_index(drop=True)


def prepare_final_df(merged, sSetFile):
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    
    final_df = merged[["idx", "lst"]].copy()
    final_df = final_df.assign(
        indate=current_time,
        send_filename=sSetFile
    )[["indate", "send_filename", "idx", "lst"]]
    
    return final_df


def insert_bcpdata(connection, final_df):
    insert_query = """
    INSERT INTO TBL_FOSS_BCPDATA (indate, send_filename, idx, lst)
    VALUES (:indate, :send_filename, :idx, :lst)
    """
    for _, row in final_df.iterrows():
        connection.execute(
            text(insert_query),
            {
                "indate": row["indate"],
                "send_filename": row["send_filename"],
                "idx": row["idx"],
                "lst": row["lst"],
            },
        )
