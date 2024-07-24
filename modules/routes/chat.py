import os
from flask import Blueprint, request, jsonify, Flask
from flask_cors import CORS
from modules.db.chat_db import ChatDBConnector
from modules.llm.llama import LlamaModel
from modules.llm.chat_gpt import GPTModel

app = Flask(__name__)
CORS(app, supports_credentials=True)

chat_bp = Blueprint('chat', __name__)

# OpenAI API 키 설정
openai_api_key = os.getenv("OPENAI_API_KEY")
if not openai_api_key:
    raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
gpt_model = GPTModel(api_key=openai_api_key, model_id="gpt-3.5-turbo")

# 키워드를 변수로 저장
KEYWORDS = ["죄송합니다", "모르겠습니다", "잘 모르겠","gpt help"]

# 서버 시작 시 LlamaModel 초기화
llama_model = LlamaModel()

def evaluate_response(response: str, keywords: list) -> bool:
    """응답에 특정 키워드가 포함되어 있는지 확인"""
    return any(keyword in response for keyword in keywords)

@chat_bp.route('/ask', methods=['POST'])
def ask_question():
    try:
        data = request.get_json()
        question = data.get('question')
        chatroom_id = data.get('chatroom_id')
        user_id = data.get('user_id')

        if not question or not chatroom_id or not user_id:
            return jsonify({'error': 'No question, chatroom_id or user_id provided'}), 400

        db_connector = ChatDBConnector()
        try:
            chat_history = db_connector.get_chat_history(chatroom_id)
            formatted_chat_history = "\n".join([f"{entry['speaker']}: {entry['message']}" for entry in chat_history])
            
            llama_response = llama_model.generate_response(f"{formatted_chat_history}\nUser: {question}")

            if evaluate_response(llama_response, KEYWORDS):
                gpt_response = gpt_model.generate(f"{formatted_chat_history}\nUser: {question}")
                db_connector.save_chat_history(chatroom_id, 'user', question)
                db_connector.save_chat_history(chatroom_id, 'gpt', gpt_response)
                return jsonify({'response': gpt_response})
            else:
                db_connector.save_chat_history(chatroom_id, 'user', question)
                db_connector.save_chat_history(chatroom_id, 'llama', llama_response)
                return jsonify({'response': llama_response})
        finally:
            db_connector.close()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@chat_bp.route('/delete-chatroom', methods=['POST'])
def delete_chatroom():
    try:
        data = request.get_json()
        chatroom_id = data.get('chatroom_id')
        user_id = data.get('user_id')

        if not chatroom_id or not user_id:
            return jsonify({'error': '채팅방 ID와 유저 ID가 제공되지 않았습니다.'}), 400

        db_connector = ChatDBConnector()
        try:
            db_connector.delete_chatroom(chatroom_id)
        finally:
            db_connector.close()

        return jsonify({'message': '채팅방 삭제 완료', 'chatroom_id': chatroom_id})
    except Exception as e:
        return jsonify({'error': f'채팅방 삭제 중 오류 발생: {str(e)}'}), 500

@chat_bp.route('/history', methods=['GET'])
def get_history():
    chatroom_id = request.args.get('chatroom_id')
    if not chatroom_id:
        return jsonify({'error': '채팅방 ID가 제공되지 않았습니다.'}), 400

    db_connector = ChatDBConnector()
    try:
        chat_history = db_connector.get_chat_history(chatroom_id)
    finally:
        db_connector.close()

    return jsonify({'chat_history': chat_history})

app.register_blueprint(chat_bp)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=30000, debug=False)
