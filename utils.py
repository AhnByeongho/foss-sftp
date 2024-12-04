import pandas as pd
import numpy as np
import os
import csv
from sqlalchemy import create_engine, text
from io import StringIO
from datetime import datetime


def get_sqlalchemy_connection(db_config):
    """
    Returns a SQLAlchemy engine for SQL Server connection.
    """
    connection_url = (
        f"mssql+pyodbc://{db_config['username']}:{db_config['password']}@"
        f"{db_config['server']}/{db_config['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    return create_engine(connection_url)


# def log_batch_record(connection, log_type, batch_spid, running_key, return_result=None, return_msg=None):
#     """
#     Logs batch process start ('S') or end ('E') in the database, mimicking SP_BATCH_LOG_RECORD.

#     :param connection: Database connection object
#     :param log_type: 'S' for start, 'E' for end
#     :param batch_spid: Batch process ID (integer)
#     :param running_key: Unique running key (string)
#     :param return_result: Result of the batch process ('success' or 'fail'), optional
#     :param return_msg: Additional message describing the result, optional
#     """
#     try:
#         if not batch_spid or not running_key.strip():
#             # Skip logging if batch_spid or running_key is invalid
#             return

#         cursor = connection.cursor()

#         if log_type == 'S':
#             # Insert start log
#             query = """
#                 INSERT INTO TBL_BATCH_PROCESSING_LOG (batchspid, runningkey, starttime)
#                 VALUES (?, ?, GETDATE())
#             """
#             cursor.execute(query, batch_spid, running_key)
#         elif log_type == 'E':
#             # Update end log
#             query = """
#                 UPDATE TBL_BATCH_PROCESSING_LOG
#                 SET endtime = GETDATE(),
#                     returnresult = ?,
#                     returnmsg = ?
#                 WHERE batchspid = ?
#                     AND runningkey = ?
#             """
#             cursor.execute(query, return_result, return_msg, batch_spid, running_key)
#         else:
#             raise ValueError("Invalid log_type. Use 'S' for start or 'E' for end.")

#         connection.commit()
#         print(f"Batch log recorded successfully for log_type={log_type}, batch_spid={batch_spid}, running_key={running_key}.")
#     except Exception as e:
#         print(f"An error occurred while logging batch record: {e}")
#         connection.rollback()
#         raise


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
                query = """
                SELECT TOP 1 trddate
                FROM (
                    SELECT trddate, holiday_yn, 
                            ROW_NUMBER() OVER (PARTITION BY holiday_yn ORDER BY trddate DESC) AS holiday_num
                    FROM TBL_HOLIDAY
                    WHERE trddate <= :target_date
                ) AS S1
                WHERE S1.holiday_yn = 'N' AND S1.holiday_num = 2
                """
                result = connection.execute(
                    text(query), {"target_date": target_date}
                ).scalar()

                if result:
                    sFndDate = result
                else:
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

                # TMP_RISKGRADE 데이터프레임 생성
                tmp_riskgrade_query = """
                SELECT 
                    auth_id, 
                    port_cd, 
                    RIGHT(port_cd, 1) AS risk_grade,
                    prd_gb
                FROM TBL_RESULT_MPLIST
                WHERE auth_id = :auth_id
                GROUP BY auth_id, port_cd, prd_gb
                """
                TMP_RISKGRADE = pd.read_sql(
                    text(tmp_riskgrade_query),
                    engine.connect(),
                    params={"auth_id": "foss"},
                )

                # TMP_RETURN 데이터프레임 생성
                terms = [
                    ("1d", sFndDate, sFndDate),
                    (
                        "1m",
                        (
                            pd.to_datetime(sFndDate)
                            - pd.DateOffset(months=1)
                            + pd.DateOffset(days=1)
                        ).strftime("%Y%m%d"),
                        sFndDate,
                    ),
                    (
                        "3m",
                        (
                            pd.to_datetime(sFndDate)
                            - pd.DateOffset(months=3)
                            + pd.DateOffset(days=1)
                        ).strftime("%Y%m%d"),
                        sFndDate,
                    ),
                    (
                        "6m",
                        (
                            pd.to_datetime(sFndDate)
                            - pd.DateOffset(months=6)
                            + pd.DateOffset(days=1)
                        ).strftime("%Y%m%d"),
                        sFndDate,
                    ),
                    (
                        "1y",
                        (
                            pd.to_datetime(sFndDate)
                            - pd.DateOffset(years=1)
                            + pd.DateOffset(days=1)
                        ).strftime("%Y%m%d"),
                        sFndDate,
                    ),
                    ("all", "20181203", sFndDate),
                ]
                valid_terms = [
                    (term, start, end)
                    for term, start, end in terms
                    if pd.to_datetime(start, format="%Y%m%d", errors="coerce")
                    >= pd.to_datetime("20181204", format="%Y%m%d")
                    or term == "all"
                ]
                term_df = pd.DataFrame(
                    valid_terms, columns=["term", "start_dt", "end_dt"]
                )

                result_return_query = """
                SELECT auth_id, port_cd, trddate, rtn_1d
                FROM TBL_RESULT_RETURN
                """
                TBL_RESULT_RETURN = pd.read_sql(
                    text(result_return_query), engine.connect()
                )

                all_returns = []

                # term_df를 사용해 TBL_RESULT_RETURN을 필터링하고 term, start_dt, end_dt 추가
                for _, term_row in term_df.iterrows():
                    start_dt = term_row["start_dt"]
                    end_dt = term_row["end_dt"]
                    term = term_row["term"]

                    # 기간 조건에 맞는 데이터를 필터링
                    filtered_return = TBL_RESULT_RETURN[
                        (TBL_RESULT_RETURN["trddate"] >= start_dt)
                        & (TBL_RESULT_RETURN["trddate"] <= end_dt)
                    ].copy()
                    filtered_return["term"] = term
                    filtered_return["start_dt"] = start_dt
                    filtered_return["end_dt"] = end_dt

                    all_returns.append(filtered_return)

                # S3: 모든 필터링된 데이터를 병합
                S3 = pd.concat(all_returns, ignore_index=True)

                # TMP_RISKGRADE와 S3를 LEFT OUTER JOIN
                TMP_RETURN = pd.merge(
                    TMP_RISKGRADE,  # S1
                    S3,  # S3
                    on=["auth_id", "port_cd"],  # JOIN 조건
                    how="left",  # LEFT OUTER JOIN
                    suffixes=("", "_extra"),
                )

                # 로그 수익률 계산
                TMP_RETURN["log_rt"] = np.log(TMP_RETURN["rtn_1d"] + 1)

                TMP_RETURN = TMP_RETURN[
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
                        "log_rt",
                    ]
                ]

                # TMP_PERFORMANCE 데이터프레임 생성
                TMP_PERFORMANCE = TMP_RETURN.groupby(
                    ["auth_id", "term", "risk_grade", "prd_gb"], as_index=False
                ).agg(total_log_rt=("log_rt", "sum"))

                # total_rt 계산: EXP(SUM(log_rt)) * 100 - 100
                TMP_PERFORMANCE["total_rt"] = (
                    (np.exp(TMP_PERFORMANCE["total_log_rt"]) * 100) - 100
                ).round(2)

                TMP_PERFORMANCE = TMP_PERFORMANCE[
                    ["auth_id", "term", "risk_grade", "prd_gb", "total_rt"]
                ]

                # TMP_PERFORMANCE에서 term별 데이터 분리
                tmp_1m = TMP_PERFORMANCE[TMP_PERFORMANCE["term"] == "1m"].copy()
                tmp_3m = TMP_PERFORMANCE[TMP_PERFORMANCE["term"] == "3m"].copy()
                tmp_6m = TMP_PERFORMANCE[TMP_PERFORMANCE["term"] == "6m"].copy()
                tmp_1y = TMP_PERFORMANCE[TMP_PERFORMANCE["term"] == "1y"].copy()
                tmp_all = TMP_PERFORMANCE[TMP_PERFORMANCE["term"] == "all"].copy()
                tmp_1d = TMP_PERFORMANCE[TMP_PERFORMANCE["term"] == "1d"].copy()

                # LEFT JOIN 수행
                merged = tmp_1d.merge(
                    tmp_1m,
                    on=["risk_grade", "prd_gb"],
                    suffixes=("", "_1m"),
                    how="left",
                )
                merged = merged.merge(
                    tmp_3m,
                    on=["risk_grade", "prd_gb"],
                    suffixes=("", "_3m"),
                    how="left",
                )
                merged = merged.merge(
                    tmp_6m,
                    on=["risk_grade", "prd_gb"],
                    suffixes=("", "_6m"),
                    how="left",
                )
                merged = merged.merge(
                    tmp_1y,
                    on=["risk_grade", "prd_gb"],
                    suffixes=("", "_1y"),
                    how="left",
                )
                merged = merged.merge(
                    tmp_all,
                    on=["risk_grade", "prd_gb"],
                    suffixes=("", "_all"),
                    how="left",
                )

                # 열 계산
                def map_expected_return(row):
                    if row["risk_grade"] == "1":
                        return "9.10" if row["prd_gb"] == "f12" else "9.04"
                    elif row["risk_grade"] == "2":
                        return "12.40" if row["prd_gb"] == "f12" else "12.53"
                    elif row["risk_grade"] == "3":
                        return "16.02" if row["prd_gb"] == "f12" else "16.64"
                    elif row["risk_grade"] == "4":
                        return "19.14" if row["prd_gb"] == "f12" else "19.80"
                    elif row["risk_grade"] == "5":
                        return "21.15" if row["prd_gb"] == "f12" else "22.99"
                    return ""

                def map_volatility(row):
                    if row["risk_grade"] == "1":
                        return "3.20" if row["prd_gb"] == "f12" else "3.10"
                    elif row["risk_grade"] == "2":
                        return "4.46" if row["prd_gb"] == "f12" else "4.57"
                    elif row["risk_grade"] == "3":
                        return "6.68" if row["prd_gb"] == "f12" else "6.87"
                    elif row["risk_grade"] == "4":
                        return "8.60" if row["prd_gb"] == "f12" else "9.21"
                    elif row["risk_grade"] == "5":
                        return "10.90" if row["prd_gb"] == "f12" else "11.51"
                    return ""

                merged["expected_return"] = merged.apply(map_expected_return, axis=1)
                merged["volatility"] = merged.apply(map_volatility, axis=1)

                # 데이터 정렬
                merged = merged.sort_values(by=["prd_gb", "risk_grade"]).reset_index(
                    drop=True
                )

                # 데이터 정리 및 생성
                merged["lst"] = (
                    sFndDate
                    + ";"
                    + merged["risk_grade"]
                    + ";"
                    + merged["prd_gb"].map({"f12": "77", "f11": "61"})
                    + ";"
                    + merged["total_rt"].fillna("").astype(str)
                    + ";"
                    + merged["total_rt_3m"].fillna("").astype(str)
                    + ";"
                    + merged["total_rt_6m"].fillna("").astype(str)
                    + ";"
                    + merged["total_rt_1y"].fillna("").astype(str)
                    + ";"
                    + merged["total_rt_all"].fillna("").astype(str)
                    + ";"
                    + merged["expected_return"]
                    + ";"
                    + merged["volatility"]
                    + ";"
                    + merged["total_rt_1m"].fillna("").astype(str)
                    + ";"
                )

                # idx 컬럼 생성 (정렬 이후)
                merged["idx"] = merged.index + 1

                # 필요한 열 추출
                final_df = merged[["idx", "lst"]].copy()
                final_df["indate"] = datetime.now().strftime("%Y%m%d%H%M%S")
                final_df["send_filename"] = sSetFile
                final_df = final_df[["indate", "send_filename", "idx", "lst"]]

                # TBL_FOSS_BCPDATA 테이블에 데이터 삽입
                # 데이터 삽입
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

                # 쿼리 실행 및 DataFrame 생성
                raw_df = pd.read_sql(
                    text(query),
                    connection,
                    params={"target_date": target_date, "auth_id": "foss"},
                )

                # pandas에서 datalength 및 substring 처리
                # fund_nm 길이 100 초과 시 자르기
                raw_df["fund_nm"] = raw_df["fund_nm"].fillna("")
                raw_df["fund_nm"] = raw_df["fund_nm"].apply(
                    lambda x: x[:100] if len(x) > 100 else x
                )

                # RIGHT(S1.port_cd, 1)
                raw_df["port_cd_last_char"] = raw_df["port_cd"].str[-1]

                # prd_gb 매핑 (CASE WHEN 처리)
                raw_df["prd_gb_mapped"] = raw_df["prd_gb"].map(
                    {"f12": "77", "f11": "61"}
                )

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

                # 필요한 컬럼만 선택
                indate = datetime.now().strftime("%Y%m%d%H%M%S")
                send_filename = sSetFile

                final_df = raw_df[["idx", "lst"]].copy()
                final_df["indate"] = indate
                final_df["send_filename"] = send_filename
                final_df = final_df[["indate", "send_filename", "idx", "lst"]]
                final_df = final_df.sort_values(by="idx").reset_index(drop=True)

                # TBL_FOSS_BCPDATA 테이블에 데이터 삽입
                # 데이터 삽입
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
                rebal_day_query = """
                SELECT 
                    CASE WHEN COUNT(*) = 0 THEN 'N' ELSE 'Y' END AS rebal_day_yn
                FROM TBL_RESULT_MPLIST 
                WHERE auth_id = :auth_id 
                    AND rebal_date = :target_date 
                    AND prd_gb = :prd_gb
                """
                # 연금(f12) 리밸런싱 여부
                result_f12 = connection.execute(
                    text(rebal_day_query),
                    {"auth_id": "foss", "target_date": target_date, "prd_gb": "f12"},
                ).scalar()
                sRebalDayYN = result_f12

                # 일반(f11) 리밸런싱 여부
                result_f11 = connection.execute(
                    text(rebal_day_query),
                    {"auth_id": "foss", "target_date": target_date, "prd_gb": "f11"},
                ).scalar()
                sRebalDayYN2 = result_f11

                i_opent_day = 3  # 영업일

                # 연금(f12) 다음 리밸런싱 날짜 계산
                query_next_rebal_date_pension = """
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
                result_pension = connection.execute(
                    text(query_next_rebal_date_pension),
                    {"target_date": target_date, "i_opent_day": i_opent_day},
                ).fetchone()

                next_rebal_date_pension = result_pension[0] if result_pension else ""

                # 일반(f11) 다음 리밸런싱 날짜 계산
                query_next_rebal_date_general = """
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
                result_general = connection.execute(
                    text(query_next_rebal_date_general),
                    {"target_date": target_date, "i_opent_day": i_opent_day},
                ).fetchone()

                next_rebal_date_general = result_general[0] if result_general else ""

                # 강제 리밸런싱 날짜 (Disable된 상태, 필요한 경우만 활성화)
                current_date = datetime.now().strftime("%Y%m%d")
                if forced_rebal_dates is not None:
                    if current_date in forced_rebal_dates:
                        sRebalDayYN = "Y"
                        sRebalDayYN2 = "Y"

                # TBL_FOSS_CUSTOMERACCOUNT에서 데이터 조회 및 처리
                query_rebalcus = """
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

                # 연금(f12) 데이터 조회
                pension_data = pd.read_sql(
                    text(query_rebalcus),
                    connection,
                    params={
                        "indate": datetime.now().strftime("%Y%m%d%H%M%S"),
                        "send_filename": sSetFile,
                        "rebal_day_yn": sRebalDayYN,
                        "next_rebal_date": next_rebal_date_pension,
                        "target_date": target_date,
                        "investgb": "77",
                    },
                )

                # 일반(f11) 데이터 조회
                general_data = pd.read_sql(
                    text(query_rebalcus),
                    connection,
                    params={
                        "indate": datetime.now().strftime("%Y%m%d%H%M%S"),
                        "send_filename": sSetFile,
                        "rebal_day_yn": sRebalDayYN2,
                        "next_rebal_date": next_rebal_date_general,
                        "target_date": target_date,
                        "investgb": "61",
                    },
                )

                # 데이터 결합 (UNION ALL)
                final_rebalcus_data = pd.concat(
                    [pension_data, general_data], ignore_index=True
                )

                # ROW_NUMBER() 기능 재현
                final_rebalcus_data["idx"] = range(1, len(final_rebalcus_data) + 1)

                final_rebalcus_data = final_rebalcus_data.sort_values(
                    by="idx"
                ).reset_index(drop=True)

                # TBL_FOSS_BCPDATA 테이블에 데이터 삽입
                # 데이터 삽입
                insert_query = """
                INSERT INTO TBL_FOSS_BCPDATA (indate, send_filename, idx, lst)
                VALUES (:indate, :send_filename, :idx, :lst)
                """
                for _, row in final_rebalcus_data.iterrows():
                    connection.execute(
                        text(insert_query),
                        {
                            "indate": row["indate"],
                            "send_filename": row["send_filename"],
                            "idx": row["idx"],
                            "lst": row["lst"],
                        },
                    )
                print(
                    "Data(ap_reval_yn) has been inserted into the TBL_FOSS_BCPDATA table."
                )

                # 수동 리벨런싱 (특정 일자에 해당 고객만 강제 리밸런싱)
                if manual_customer_ids is not None and manual_rebal_yn is not None:
                    # 수동 리밸런싱 신호 전송 작업 수행
                    for customer_id in manual_customer_ids:
                        # 업데이트할 lst 값 생성
                        updated_lst_value = (
                            f"{customer_id};{manual_rebal_yn};{target_date};"
                        )

                        # 데이터프레임 내 업데이트
                        final_rebalcus_data.loc[
                            final_rebalcus_data["lst"].str.startswith(
                                f"{customer_id};"
                            ),
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
                    print(
                        f"Manual rebalancing applied and updated in TBL_FOSS_BCPDATA for customers: {manual_customer_ids}"
                    )

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


def process_mp_info_eof(conn, target_date, sftp_client):
    try:
        cursor = conn.cursor()

        # 기준 날짜 설정
        if not target_date:
            target_date = datetime.now().strftime("%Y%m%d")
        print(f"Processing MP_INFO_EOF for target date: {target_date}")

        # 파일 이름 설정
        sSetFile = f"mp_info_eof.{target_date}"

        # #FTPResult 테이블에 데이터 삽입
        cursor.execute(
            """
        INSERT INTO #FTPResult (d_code, send_filename, rst)
        VALUES ('BATCH_FOSS_08', ?, '.')
        """,
            sSetFile,
        )

        # 빈 EOF 파일 생성
        local_file_path = f"{sSetFile}.csv"
        with open(local_file_path, "w", newline="", encoding="utf-8") as file:
            # EOF 파일은 비어 있는 상태로 저장
            file.write("")
        print(f"Empty EOF file {local_file_path} generated successfully.")

        # SFTP 업로드
        with sftp_client.open(f"/robo_data/{sSetFile}", "w") as sftp_file:
            with open(local_file_path, "r", encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())
        print(f"EOF file {sSetFile} uploaded to SFTP successfully.")

        # 전송 상태 업데이트
        cursor.execute(
            """
        UPDATE #FTPResult
        SET rst = 'bcp create success'
        WHERE send_filename = ?
        """,
            sSetFile,
        )

        conn.commit()

    except Exception as e:
        print(f"An error occurred during process_mp_info_eof: {e}")
        cursor.execute(
            """
        UPDATE #FTPResult
        SET rst = 'bcp create failed'
        WHERE send_filename = ?
        """,
            sSetFile,
        )
        conn.rollback()
        raise

    finally:
        # 생성된 로컬 파일 삭제
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local EOF file {local_file_path} deleted.")


# def log_ftp_process(conn, batch_spid, running_key):
#     try:
#         cursor = conn.cursor()

#         # FTPResult 테이블 데이터가 있는지 확인
#         cursor.execute("SELECT COUNT(*) FROM #FTPResult")
#         ftp_result_count = cursor.fetchone()[0]

#         if ftp_result_count > 0:
#             # 현재 시각을 VARCHAR 형식으로 변환
#             dt_now = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]

#             # TBL_EVENT_LOG에 데이터 삽입
#             cursor.execute("""
#             INSERT INTO TBL_EVENT_LOG (eventdate, eventtype, call_pgm_name, message, result)
#             SELECT
#                 ?, -- eventdate
#                 d_code,
#                 'Python: log_ftp_process',
#                 CONCAT(rst, '      ', send_filename),
#                 CASE WHEN CHARINDEX('success', rst) = 0 THEN 'false' ELSE 'true' END
#             FROM #FTPResult
#             """, dt_now)
#             print("FTP processing results logged into TBL_EVENT_LOG successfully.")

#         # 배치 로그 성공 처리
#         cursor.execute("""
#         EXEC SP_BATCH_LOG_RECORD 'E', ?, ?, '', 'success', '데이터 처리 성공'
#         """, (batch_spid, running_key))
#         conn.commit()

#     except Exception as e:
#         print("LOG SAVE Error:", e)
#         conn.rollback()

#         # 배치 로그 실패 처리
#         cursor.execute("""
#         EXEC SP_BATCH_LOG_RECORD 'E', ?, ?, '', 'fail', 'LOG SAVE Error'
#         """, (batch_spid, running_key))
#         conn.commit()
