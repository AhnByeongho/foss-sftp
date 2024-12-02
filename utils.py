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

        # 최근 영업일 계산
        with engine.connect() as connection:
            with connection.begin():
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

                # CSV 파일 저장
                local_file_path = (
                    f"/Users/mac/Downloads/{sSetFile}.csv"  # 로컬 경로 설정
                )
                final_df[["lst"]].to_csv(
                    local_file_path, index=False, header=False, encoding="utf-8"
                )

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


def process_mp_list(conn, target_date, sftp_client):
    try:
        cursor = conn.cursor()

        # 기준 날짜 설정
        if not target_date:
            target_date = datetime.now().strftime("%Y%m%d")
        print(f"Processing MP List for target date: {target_date}")

        sSetFile = f"mp_fnd_info.{target_date}"
        local_file_path = f"{sSetFile}.csv"
        sftp_target_path = f"/home/fossDev/robo_data/{sSetFile}"

        # FTPResult 테이블 데이터 삽입
        cursor.execute(
            """
        INSERT INTO #FTPResult (d_code, send_filename, rst)
        VALUES ('BATCH_FOSS_05', ?, '.')
        """,
            sSetFile,
        )

        # TBL_FOSS_BCPDATA 데이터 삽입
        cursor.execute(
            """
        INSERT INTO TBL_FOSS_BCPDATA (indate, send_filename, idx, lst)
        SELECT ?, ?, 
                ROW_NUMBER() OVER (ORDER BY S1.port_cd ASC),
                RIGHT(S1.port_cd, 1) + ';' + 
                CASE S1.prd_gb
                    WHEN 'f12' THEN '77'
                    WHEN 'f11' THEN '61'
                END + ';' + 
                S1.prd_cd + ';' + 
                CASE
                    WHEN DATALENGTH(ISNULL(S2.fund_nm, '')) > 100 THEN LEFT(S2.fund_nm, 100)
                    ELSE ISNULL(S2.fund_nm, '')
                END + ';' + 
                CAST(CAST(S1.prd_weight / 100 AS NUMERIC(4, 2)) AS VARCHAR(4)) + ';'
        FROM TBL_RESULT_MPLIST S1
        LEFT OUTER JOIN TBL_FOSS_UNIVERSE S2 
            ON S2.fund_cd = S1.prd_cd AND S2.trddate = (SELECT MAX(trddate) FROM TBL_FOSS_UNIVERSE)
        INNER JOIN (
            SELECT port_cd, MAX(rebal_date) AS rebal_date
            FROM TBL_RESULT_MPLIST
            WHERE auth_id = 'foss' AND rebal_date <= ?
            GROUP BY port_cd
        ) S3 
            ON S3.port_cd = S1.port_cd AND S3.rebal_date = S1.rebal_date
        WHERE S1.auth_id = 'foss'
        ORDER BY S1.port_cd ASC
        """,
            (datetime.now().strftime("%Y%m%d%H%M%S"), sSetFile, target_date),
        )

        print("MP List data inserted into TBL_FOSS_BCPDATA successfully.")

        # BCP 데이터 조회 및 로컬 파일로 저장
        cursor.execute(
            """
        SELECT lst 
        FROM TBL_FOSS_BCPDATA
        WHERE send_filename = ?
        ORDER BY idx ASC
        """,
            sSetFile,
        )

        rows = cursor.fetchall()
        if not rows:
            print("No data found to save in the file.")
            return

        # 파일 생성
        with open(local_file_path, "w", newline="", encoding="utf-8") as file:
            for row in rows:
                file.write(row[0] + "\n")  # 'lst' 컬럼 값만 저장

        print(f"Local file {local_file_path} generated successfully.")

        # SFTP 업로드
        print(f"Uploading file to SFTP path: {sftp_target_path}")
        with sftp_client.open(sftp_target_path, "w") as sftp_file:
            with open(local_file_path, "r", encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())
        print(f"File {sSetFile} uploaded to SFTP successfully.")

        # TBL_FOSS_BCPDATA 처리 상태 업데이트
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
        print(f"An error occurred during process_mp_list: {e}")
        conn.rollback()

    finally:
        # 파일 삭제 확인
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local file {local_file_path} deleted.")


def process_rebalcus(
    conn, target_date, sftp_client, manual_customer_ids=None, manual_rebal_yn=None
):
    try:
        cursor = conn.cursor()

        # 기준 날짜 설정
        if not target_date:
            target_date = datetime.now().strftime("%Y%m%d")
        print(f"Processing Rebalancing Data for target date: {target_date}")

        # 파일 이름 설정
        sSetFile = f"ap_reval_yn.{target_date}"
        sInDate = datetime.now().strftime("%Y%m%d%H%M%S")

        # 리밸런싱 여부 확인
        cursor.execute(
            """
        SELECT CASE WHEN COUNT(*) = 0 THEN 'N' ELSE 'Y' END 
        FROM TBL_RESULT_MPLIST
        WHERE auth_id = 'foss' AND rebal_date = ? AND prd_gb = 'f12'
        """,
            target_date,
        )
        sRebalDayYN = cursor.fetchone()[0]

        cursor.execute(
            """
        SELECT CASE WHEN COUNT(*) = 0 THEN 'N' ELSE 'Y' END 
        FROM TBL_RESULT_MPLIST
        WHERE auth_id = 'foss' AND rebal_date = ? AND prd_gb = 'f11'
        """,
            target_date,
        )
        sRebalDayYN2 = cursor.fetchone()[0]

        # 다음 리밸런싱 날짜 계산
        iOpentDay = 3
        cursor.execute(
            """
        SELECT MIN(trddate)
        FROM (
            SELECT trddate, 
                    ROW_NUMBER() OVER (PARTITION BY LEFT(trddate, 6) ORDER BY trddate ASC) AS MonthCnt
            FROM TBL_HOLIDAY
            WHERE LEFT(trddate, 6) >= LEFT(?, 6)
                AND holiday_yn = 'N'
                AND SUBSTRING(trddate, 5, 2) IN ('01', '04', '07', '10')
        ) AS S1
        WHERE trddate >= ? AND MonthCnt = ?
        """,
            (target_date, target_date, iOpentDay),
        )
        sNRebalDate = cursor.fetchone()[0]

        cursor.execute(
            """
        SELECT MIN(trddate)
        FROM (
            SELECT trddate, 
                    ROW_NUMBER() OVER (PARTITION BY LEFT(trddate, 6) ORDER BY trddate ASC) AS MonthCnt
            FROM TBL_HOLIDAY
            WHERE LEFT(trddate, 6) >= LEFT(?, 6)
                AND holiday_yn = 'N'
                AND SUBSTRING(trddate, 5, 2) IN ('01', '04', '07', '10')
        ) AS S1
        WHERE trddate >= ? AND MonthCnt = ?
        """,
            (target_date, target_date, iOpentDay),
        )
        sNRebalDate2 = cursor.fetchone()[0]

        # 리밸런싱 데이터 생성
        cursor.execute(
            """
        SELECT customer_id, 
                customer_id + ';' + 
                CASE WHEN ? = 'N' THEN 'N'
                    ELSE CASE WHEN ? = 'Y' AND order_status IN ('Y', 'Y1', 'Y3') THEN 'Y' ELSE 'N' END
                END + ';' + ? + ';' AS lst
        FROM TBL_FOSS_CUSTOMERACCOUNT
        WHERE trddate = ? AND investgb = '77'
        UNION ALL
        SELECT customer_id, 
                customer_id + ';' + 
                CASE WHEN ? = 'N' THEN 'N'
                    ELSE CASE WHEN ? = 'Y' AND order_status IN ('Y', 'Y1', 'Y3') THEN 'Y' ELSE 'N' END
                END + ';' + ? + ';' AS lst
        FROM TBL_FOSS_CUSTOMERACCOUNT
        WHERE trddate = ? AND investgb = '61'
        """,
            (
                sRebalDayYN,
                sRebalDayYN,
                sNRebalDate,
                target_date,
                sRebalDayYN2,
                sRebalDayYN2,
                sNRebalDate2,
                target_date,
            ),
        )
        rows = cursor.fetchall()

        # ROW_NUMBER()와 동일한 방식으로 idx 생성
        processed_rows = []
        for idx, (customer_id, lst) in enumerate(rows, start=1):
            processed_rows.append((sInDate, sSetFile, idx, lst))

        # 중복 데이터 확인 및 삽입
        insert_query = """
        INSERT INTO TBL_FOSS_BCPDATA (indate, send_filename, idx, lst)
        VALUES (?, ?, ?, ?)
        """
        for row in processed_rows:
            try:
                cursor.execute(insert_query, row)
            except Exception as e:
                print(f"Duplicate entry skipped: {row} | Error: {e}")
                continue

        # 수동 리밸런싱 처리
        if manual_customer_ids and manual_rebal_yn:
            customer_ids = manual_customer_ids.split(",")
            print(
                f"Manual rebalancing for customers: {customer_ids} with rebal_yn: {manual_rebal_yn}"
            )
            for customer_id in customer_ids:
                cursor.execute(
                    """
                UPDATE TBL_FOSS_BCPDATA 
                SET lst = LEFT(lst, CHARINDEX(';', lst) - 1) + ';' + ? + ';' + ?
                WHERE send_filename = ? 
                    AND indate = ? 
                    AND LEFT(lst, CHARINDEX(';', lst) - 1) = ?
                """,
                    (manual_rebal_yn, target_date, sSetFile, sInDate, customer_id),
                )

        # CSV 파일 저장
        local_file_path = f"{sSetFile}.csv"
        with open(local_file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerows([(row[3],) for row in processed_rows])  # lst 컬럼만 추출

        print(f"Data saved locally to {local_file_path} for verification.")

        # SFTP 업로드
        with sftp_client.open(f"/robo_data/{sSetFile}", "w") as sftp_file:
            with open(local_file_path, "r", encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())
        print(f"File {sSetFile} uploaded successfully.")

        conn.commit()

    except Exception as e:
        print(f"An error occurred during process_rebalcus: {e}")
        conn.rollback()

    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local file {local_file_path} deleted.")


def process_report(conn, target_date, sftp_client):
    try:
        cursor = conn.cursor()

        # 파일 이름 설정
        sSetFile = f"report.{target_date}"
        print(f"Processing report for target date: {target_date}, File: {sSetFile}")

        # #FTPResult 테이블 데이터 삽입
        cursor.execute(
            """
        INSERT INTO #FTPResult (d_code, send_filename, rst)
        VALUES ('BATCH_FOSS_07', ?, '.')
        """,
            sSetFile,
        )

        # TBL_FOSS_REPORT 데이터 확인
        cursor.execute("""
        SELECT COUNT(trddate)
        FROM TBL_FOSS_REPORT
        WHERE trddate = CONVERT(CHAR(8), GETDATE(), 112)
        """)
        report_count = cursor.fetchone()[0]

        if report_count >= 1:
            # TBL_FOSS_BCPDATA 데이터 삽입
            cursor.execute(
                """
            INSERT INTO TBL_FOSS_BCPDATA (indate, send_filename, idx, lst)
            SELECT ?, ?, 
                    ROW_NUMBER() OVER (ORDER BY (SELECT 1)),
                    trddate + ';' + 
                    REPLACE(REPLACE(REPLACE(REPLACE(performance_t,'"','&quot;'), CHAR(13), '\n'), CHAR(10), ''), ';', '') + ';' + 
                    REPLACE(REPLACE(REPLACE(REPLACE(performance_c,'"','&quot;'), CHAR(13), '\n'), CHAR(10), ''), ';', '') + ';'
            FROM TBL_FOSS_REPORT
            WHERE trddate = (
                SELECT MAX(trddate)
                FROM TBL_FOSS_REPORT
                WHERE trddate <= CONVERT(VARCHAR(8), GETDATE(), 112)
            )
            """,
                (datetime.now().strftime("%Y%m%d%H%M%S"), sSetFile),
            )
            print("Report data inserted into TBL_FOSS_BCPDATA successfully.")

        # BCP 데이터 조회 및 로컬 파일 저장
        cursor.execute(
            """
        SELECT lst 
        FROM TBL_FOSS_BCPDATA
        WHERE send_filename = ?
        ORDER BY idx
        """,
            sSetFile,
        )

        rows = cursor.fetchall()
        local_file_path = f"{sSetFile}.csv"
        with open(local_file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["lst"])  # 헤더
            writer.writerows(rows)

        print(f"Local file {local_file_path} generated successfully.")

        # SFTP 업로드
        with sftp_client.open(f"/robo_data/{sSetFile}", "w") as sftp_file:
            with open(local_file_path, "r", encoding="utf-8") as local_file:
                sftp_file.write(local_file.read())
        print(f"File {sSetFile} uploaded to SFTP successfully.")

        # TBL_FOSS_BCPDATA 처리 상태 업데이트
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
        print(f"An error occurred during process_report: {e}")
        conn.rollback()

    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local file {local_file_path} deleted.")


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
