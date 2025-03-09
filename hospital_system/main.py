from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)

client = bigquery.Client()

PROJECT_ID = "barbara2-451412"
DATASET_ID = "MIMIC"
TABLE_ID = "PATIENTS"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

#adicionar um paciente
@app.route('/rest/user', methods=['POST'])
def create_patient():
    data = request.get_json()
    print("Recebido:", data)   #testar 
    
    query = f"""
    INSERT INTO `{TABLE_REF}` (ROW_ID, SUBJECT_ID, GENDER, DOB)
    VALUES (@row_id, @subject_id, @gender, @dob)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("row_id", "INT64", data["row_id"]),
            bigquery.ScalarQueryParameter("subject_id", "INT64", data["subject_id"]),
            bigquery.ScalarQueryParameter("gender", "STRING", data["gender"]),
            bigquery.ScalarQueryParameter("dob", "TIMESTAMP", data["dob"])
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Paciente criado com sucesso!"}), 201

#atualizar informacoes do paciente
@app.route('/rest/user/<int:subject_id>', methods=['PUT'])
def update_patient(subject_id):
    data = request.get_json()

    query = f"""
    UPDATE `{TABLE_REF}`
    SET GENDER = @gender, DOB = @dob
    WHERE SUBJECT_ID = @subject_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gender", "STRING", data["gender"]),
            bigquery.ScalarQueryParameter("dob", "TIMESTAMP", data["dob"]),
            bigquery.ScalarQueryParameter("subject_id", "INT64", subject_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": f"Paciente {subject_id} atualizado com sucesso!"})

#remover paciente
@app.route('/rest/user/<int:subject_id>', methods=['DELETE'])
def delete_patient(subject_id):
    
    delete_query = f"""
    UPDATE `{TABLE_REF}`
    SET SUBJECT_ID = 999999
    WHERE SUBJECT_ID = @subject_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("subject_id", "INT64", subject_id)]
    )

    query_job = client.query(delete_query, job_config=job_config)
    query_job.result()

    return jsonify({"message": f"Paciente {subject_id} marcado como 'Deleted User'!"})

if __name__ == '__main__':
    app.run(debug=True)

