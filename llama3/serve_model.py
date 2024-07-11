from flask import Flask, request, jsonify
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from langchain.llms import OpenAI
import os

app = Flask(__name__)

# OpenAI API 키 설정
key = "APIKEY"   # 나중에 암호화 해야함
os.environ["OPENAI_API_KEY"] = key

# LLaMA-3 모델 로드
llama_model_id = 'MLP-KTLim/llama-3-Korean-Bllossom-8B'
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
llama_tokenizer = AutoTokenizer.from_pretrained(llama_model_id)
llama_model = AutoModelForCausalLM.from_pretrained(llama_model_id, torch_dtype=torch.bfloat16).to(device)

# GPT-4 모델 로드
gpt_4_model = OpenAI(model="gpt-4")

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

# GPT-4 응답 생성 함수
def generate_gpt4_response(instruction):
    response = gpt_4_model.generate(instruction)
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

@app.route('/generate_gpt4', methods=['POST'])
def generate_gpt4():
    try:
        instruction = request.json.get('instruction')
        if not instruction:
            return jsonify({'error': 'No instruction provided'}), 400
        response = generate_gpt4_response(instruction)
        return jsonify({'response': response})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=30000)
