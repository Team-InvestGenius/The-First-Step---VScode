from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

LLAMA_SERVER_URL = 'http://localhost:30000/generate_llama'
GPT4_SERVER_URL = 'http://localhost:30000/generate_gpt4'

@app.route('/ask', methods=['POST'])
def ask_question():
    try:
        data = request.get_json()
        question = data.get('question')
        if not question:
            return jsonify({'error': 'No question provided'}), 400
        
        # LLaMA-3 모델 호출
        response = requests.post(LLAMA_SERVER_URL, json={'instruction': question})
        response.raise_for_status()
        llama_response = response.json().get('response')

        # LLaMA-3 응답이 부실한지 확인 (예: 길이 또는 내용 기준)
        if len(llama_response) < 30:  # 예시로 30이하면 gpt로
            # GPT-4 모델 호출
            response = requests.post(GPT4_SERVER_URL, json={'instruction': question})
            response.raise_for_status()
            gpt4_response = response.json().get('response')
            return jsonify({'response': gpt4_response})
        else:
            return jsonify({'response': llama_response})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=30001, debug=True)
