import openai
import json

# OpenAI API 키 설정
openai.api_key = "your-api-key-here"


# 파인튜닝용 데이터 준비
def prepare_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f]


# 파인튜닝 작업 생성
def create_fine_tuning_job(training_file_id, model="gpt-3.5-turbo"):
    response = openai.FineTuningJob.create(
        training_file=training_file_id,
        model=model
    )
    return response


# 파인튜닝 작업 상태 확인
def check_fine_tuning_status(job_id):
    return openai.FineTuningJob.retrieve(job_id)


# 메인 실행 함수
def main():
    # 1. 데이터 준비
    data = prepare_data("path_to_your_jsonl_file.jsonl")

    # 2. 데이터 파일 업로드
    file = openai.File.create(
        file=open("path_to_your_jsonl_file.jsonl", "rb"),
        purpose='fine-tune'
    )

    # 3. 파인튜닝 작업 생성
    job = create_fine_tuning_job(file.id)
    print(f"Fine-tuning job created: {job.id}")

    # 4. 작업 상태 확인
    status = check_fine_tuning_status(job.id)
    print(f"Job status: {status.status}")


if __name__ == "__main__":
    main()