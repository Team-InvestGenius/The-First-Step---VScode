import os
import torch
import tempfile
from transformers import AutoTokenizer, AutoModelForCausalLM
from flask import Flask, request, jsonify
from modules.llm.chat_gpt import GPTModel


app = Flask(__name__)

# 시스템 환경 변수에 OPENAI_API_KEY 를 등록할것!
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")


# LLaMA-3 모델 로드
llama_model_id = 'MLP-KTLim/llama-3-Korean-Bllossom-8B'
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
llama_tokenizer = AutoTokenizer.from_pretrained(llama_model_id)
llama_model = AutoModelForCausalLM.from_pretrained(llama_model_id, torch_dtype=torch.bfloat16).to(device)

# GPT-3.5 모델
gpt_model = GPTModel(api_key=openai_api_key, model_id="gpt-3.5-turbo")


# LLaMA-3 응답 생성 함수
def generate_llama_response(instruction):
    PROMPT = '''You are a helpful AI assistant. Please answer the user's questions kindly. 당신은 유능한 AI 어시스턴트 입니다. 사용자의 질문에 대해 친절하게 답변해주세요.'''
    messages = [
        {"role": "system", "content": f"{PROMPT}"},
        {"role": "user", "content": f"{instruction}"}
    ]
    input_ids = llama_tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(device)
    outputs = llama_model.generate(input_ids, max_new_tokens=2048, eos_token_id=llama_tokenizer.eos_token_id, do_sample=True, temperature=0.6, top_p=0.9)
    response = llama_tokenizer.decode(outputs[0][input_ids.shape[-1]:], skip_special_tokens=True)
    return response


@app.route('/generate_llama', methods=['POST'])
def generate_llama():
    try:
        instruction = request.json.get('instruction')
        if not instruction:
            return jsonify({'error': 'No instruction provided'}), 400
        response = generate_llama_response(instruction)
        return jsonify({'response': response})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/generate', methods=['POST'])
def generate():
    """
    텍스트 생성 API 엔드포인트
    """
    try:
        instruction = request.json.get('instruction')
        if not instruction:
            return jsonify({'error': '지시사항이 제공되지 않았습니다'}), 400

        response = gpt_model.generate(instruction)
        return jsonify({'response': response})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/fine-tune', methods=['POST'])
def fine_tune():
    """
    Fine-tuning 시작 API 엔드포인트
    """
    try:
        training_file = request.files.get('file')
        if not training_file:
            return jsonify({'error': '훈련 파일이 제공되지 않았습니다'}), 400

        # 임시 파일 사용
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jsonl') as tmp:
            training_file.save(tmp.name)
            job_id, status = gpt_model.fine_tune(tmp.name)

        # 임시 파일 삭제
        os.unlink(tmp.name)

        return jsonify({'job_id': job_id, 'status': status})
    except Exception as e:
        app.logger.error(f'Fine-tuning 시작 중 오류 발생: {str(e)}')
        return jsonify({'error': str(e)}), 500


@app.route('/fine-tune-status', methods=['POST'])
def fine_tune_status():
    """
    Fine-tuning 상태 확인 API 엔드포인트

    사용방법 (예)
    response = requests.post('http://your-api-url/fine-tune-status',
                         json={'job_id': 'ft-AF1WoRqd3aJAHsqc9NY7iL8F'})
    status = response.json()
    """
    try:
        data = request.json
        if not data or 'job_id' not in data:
            return jsonify({'error': 'job_id가 제공되지 않았습니다'}), 400

        job_id = data['job_id']

        # job_id 형식 검증 (예: 'ft-'로 시작하는지)
        if not job_id.startswith('ft-'):
            return jsonify({'error': '유효하지 않은 job_id 형식입니다'}), 400

        status = gpt_model.fine_tune_status(job_id)
        return jsonify(status)
    except ValueError as ve:
        return jsonify({'error': f'잘못된 요청: {str(ve)}'}), 400
    except Exception as e:
        app.logger.error(f'Fine-tune 상태 확인 중 오류 발생: {str(e)}')
        return jsonify({'error': '서버 내부 오류가 발생했습니다'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=30000)
