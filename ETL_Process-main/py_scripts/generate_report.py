from datetime import datetime
import psycopg2


def save_fraud_analysis_to_db(analysis_result, source_files):
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="varr",
        user="var",
        password="123"
    )
    cursor = connection.cursor()

    analysis_data = {
        "total_fraud_cases": analysis_result["total_fraud_cases"],
        "most_common_fraud": analysis_result["most_common_fraud"],
        "top_fraudsters": str(analysis_result["top_fraudsters"]),
        "most_frequent_time": analysis_result["most_frequent_time"],
        "source_files": str(source_files)
    }

    cursor.execute("""
            INSERT INTO meta_fraud_analysis (total_fraud_cases, most_common_fraud, top_fraudsters, most_frequent_time, source_files)
            VALUES (%s, %s, %s, %s, %s)
        """,
        (
            analysis_data["total_fraud_cases"],
            analysis_data["most_common_fraud"],
            analysis_data["top_fraudsters"],
            analysis_data["most_frequent_time"],
            analysis_data["source_files"]
        ))

    connection.commit()
    cursor.close()
    connection.close()


def generate_report(fraud_data):
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="varr",
        user="var",
        password="123"
    )
    cursor = connection.cursor()

    for record in fraud_data.to_dict(orient="records"):
        if isinstance(record, dict):
            record_lower = {k.lower(): v for k, v in record.items()}
            cursor.execute("""
                INSERT INTO rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (
                    record_lower["event_dt"],
                    record_lower["passport"],
                    record_lower["fio"],
                    record_lower["phone"],
                    record_lower["event_type"],
                    record_lower["report_dt"]
                ))

    connection.commit()
    cursor.close()
    connection.close()


def save_fraud_analysis(analysis_result, source_files, filename="fraud_analysis.txt"):
    save_fraud_analysis_to_db(analysis_result, source_files)

    with open(filename, "a", encoding="utf-8") as file:
        file.write(f"Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        file.write(f"Источник данных:\n")
        for source in source_files:
            file.write(f"- {source}\n")
        file.write(f"Общее количество мошеннических операций: {analysis_result['total_fraud_cases']}\n")
        file.write(f"Самый частый тип мошенничества: {analysis_result['most_common_fraud']}\n")
        file.write(f"Топ-3 самых активных мошенников (по паспорту): {', '.join(analysis_result['top_fraudsters'])}\n")
        file.write(f"Чаще всего мошеннические операции происходят: {analysis_result['most_frequent_time']}\n")
        file.write("-" * 50 + "\n")
