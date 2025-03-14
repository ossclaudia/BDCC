from flask import Flask, request, jsonify
from google.cloud import bigquery
import flask
import concurrent.futures

app = Flask(__name__)

client = bigquery.Client()

PROJECT_ID = "barbara2-451412"
DATASET_ID = "MIMIC"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}"
TABLE_PATIENTS = "PATIENTS"
TABLE_ADMISSIONS = "ADMISSIONS"
TABLE_QUESTIONS = "QUESTIONS"
TABLE_ANSWERS = "ANSWERS"

@app.route('/')
def get_patients():
    query_job = client.query(
        """
        SELECT
            subject_id,
            gender,
            dob
        FROM `barbara2-451412.MIMIC.PATIENTS`
        LIMIT 10
        """
    )

    return flask.redirect(
        flask.url_for(
            "results",
            project_id=query_job.project,
            job_id=query_job.job_id,
            location=query_job.location,
        )
    )


@app.route("/results")
def results():
    project_id = flask.request.args.get("project_id")
    job_id = flask.request.args.get("job_id")
    location = flask.request.args.get("location")

    query_job = client.get_job(
        job_id,
        project=project_id,
        location=location,
    )

    try:
        # Set a timeout because queries could take longer than one minute.
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("query_result.html", results=results)


#adicionar um paciente
@app.route('/rest/user', methods=['POST'])
def create_patient():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_PATIENTS}` (SUBJECT_ID, GENDER, DOB)
    VALUES (@subject_id, @gender, @dob)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
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
    UPDATE `{TABLE_REF}.{TABLE_PATIENTS}`
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
    DELETE FROM `{TABLE_REF}.{TABLE_PATIENTS}`
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

#---------------------------------------------------------------------------------------------------

# Create Admission

@app.route('/rest/admissions', methods=['POST'])
def create_admission():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_ADMISSIONS}` (SUBJECT_ID, HADM_ID, ADMITTIME, ADMISSION_LOCATION)
    VALUES (@subject_id, @hadm_id, @admittime, @admission_location)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("subject_id", "INT64", data["subject_id"]),
            bigquery.ScalarQueryParameter("hadm_id", "INT64", data["hadm_id"]),
            bigquery.ScalarQueryParameter("admittime", "TIMESTAMP", data["admittime"]),
            bigquery.ScalarQueryParameter("admission_location", "STRING", data["admission_location"])
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Admission criada com sucesso!"}), 201

# Update Medical Event 

@app.route('/rest/admissions/<int:hadm_id>', methods=['PUT'])
def update_admission(hadm_id):
    data = request.get_json()

    dischtime = data.get("dischtime")
    deathtime = data.get("deathtime")

    if deathtime:
        dischtime = deathtime

    death_param = deathtime if deathtime else None

    query = f"""
    UPDATE `{TABLE_REF}.{TABLE_ADMISSIONS}`
    SET 
        DISCHTIME = @dischtime,
        DEATHTIME = @deathtime
    WHERE HADM_ID = @hadm_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dischtime", "TIMESTAMP", dischtime),
            bigquery.ScalarQueryParameter("deathtime", "TIMESTAMP", death_param),
            bigquery.ScalarQueryParameter("hadm_id", "INT64", hadm_id)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": f"Admission {hadm_id} atualizada com sucesso!"})


#---------------------------------------------------------------------------------------------------

# Create a question 

@app.route('/rest/questions', methods=['POST'])
def create_question():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_QUESTIONS}` (Message, ID, Patient_ID)
    VALUES (@message, @id, @patient_id)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("message", "STRING", data["message"]),
            bigquery.ScalarQueryParameter("id", "INT64", data["id"]),
            bigquery.ScalarQueryParameter("patient_id", "INT64", data["patient_id"])
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Questao criada com sucesso!"}), 201

# Reply to question

@app.route('/rest/answers', methods=['POST'])
def create_answer():
    data = request.get_json()
 
    query = f"""
    INSERT INTO `{TABLE_REF}.{TABLE_ANSWERS}` (Message, Replying_To, Unit_ID)
    VALUES (@message, @replying_to, @unit_id)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("message", "STRING", data["message"]),
            bigquery.ScalarQueryParameter("replying_to", "INT64", data["replying_to"]),
            bigquery.ScalarQueryParameter("unit_id", "STRING", data["unit_id"])
        ]
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()

    return jsonify({"message": "Questao respondida com sucesso!"}), 201

# List the questions

@app.route('/questions')
def questions():
    query_job = client.query(
        """
        SELECT
            message,
            id,
            patient_id
        FROM `barbara2-451412.MIMIC.Questions`
        LIMIT 10
        """
    )

    try:
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("query_result.html", results=results)
