from openai import OpenAI
import os


class GPTModel:
    def __init__(self, api_key, model_id="gpt-3.5-turbo"):
        """
        GPTModel 클래스 초기화
        :param api_key: OpenAI API 키
        :param model_id: 사용할 GPT 모델 ID (기본값: gpt-3.5-turbo)
        """
        self.client = OpenAI(api_key=api_key)
        self.model_id = model_id

    def generate(self, instruction):
        """
        주어진 지시에 따라 텍스트 생성
        :param instruction: 사용자의 입력 지시
        :return: 생성된 텍스트
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model_id,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": instruction}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            raise Exception(f"텍스트 생성 오류: {str(e)}")

    def fine_tune(self, file_path):
        """
        모델 fine-tuning 작업 시작
        :param file_path: 훈련 데이터 파일 경로
        :return: fine-tuning 작업 ID와 상태
        """
        try:
            # 파일 업로드
            file = self.client.files.create(file=open(file_path, "rb"), purpose="fine-tune")
            # fine-tuning 작업 생성
            job = self.client.fine_tuning.jobs.create(
                training_file=file.id,
                model=self.model_id
            )
            return job.id, job.status
        except Exception as e:
            raise Exception(f"Fine-tuning 오류: {str(e)}")

    def fine_tune_status(self, job_id):
        """
        fine-tuning 작업의 상태 확인
        :param job_id: fine-tuning 작업 ID
        :return: 작업 상태 정보
        """
        try:
            job = self.client.fine_tuning.jobs.retrieve(job_id)
            return {
                'job_id': job.id,
                'status': job.status,
                'fine_tuned_model': job.fine_tuned_model
            }
        except Exception as e:
            raise Exception(f"상태 확인 오류: {str(e)}")