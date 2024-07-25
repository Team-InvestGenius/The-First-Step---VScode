from flask import Blueprint, request, jsonify, current_app
from modules.db.chat_db import ChatDBConnector
from modules.routes.session_manager import SessionManager
from modules.llm.chat_gpt import GPTModel

chat_bp = Blueprint('chat', __name__)

# 키워드를 변수로 저장
KEYWORDS = ["죄송합니다", "모르겠습니다", "잘 모르겠", "gpt help"]

session_manager = SessionManager.get_instance()

def get_gpt_model():
    api_key = current_app.config['OPENAI_API_KEY']
    model_id = current_app.config['GPT_MODEL_ID']
    return GPTModel(api_key=api_key, model_id=model_id)

def evaluate_response(response: str, keywords: list) -> bool:
    """응답에 특정 키워드가 포함되어 있는지 확인"""
    return any(keyword in response for keyword in keywords)

@chat_bp.route('/ask', methods=['POST'])
def ask_question():
    try:
        data = request.get_json()
        question = data.get('question')
        user_id = data.get('user_id')

        if not question or not user_id:
            return jsonify({'error': 'question, or user_id 제공되지 않음'}), 400

        db_connector = ChatDBConnector()
        try:
            # 유저의 채팅룸 개수 확인 및 채팅방 생성
            chatroom_count = db_connector.get_chatroom_count_by_userid(user_id)
            if chatroom_count >= 3:  # 채팅방 3개 이상이면
                chatroom_id = db_connector.get_last_active_chatroom_by_userid(user_id)
            else:
                chatroom_id = db_connector.create_chatroom(user_id)

            chat_history = db_connector.get_chat_history(chatroom_id)
            formatted_chat_history = "\n".join([f"{entry['speaker']}: {entry['message']}" for entry in chat_history])

            llama_model = session_manager.get_model()
            llama_response = llama_model.generate_response(f"{formatted_chat_history}\nUser: {question}")

            if evaluate_response(llama_response, KEYWORDS):
                gpt_model = get_gpt_model()
                gpt_response = gpt_model.generate(f"{formatted_chat_history}\nUser: {question}")
                db_connector.save_chat_history(chatroom_id, 'user', question)
                db_connector.save_chat_history(chatroom_id, 'gpt', gpt_response)
                response = {'response': gpt_response, 'chatroom_id': chatroom_id}
            else:
                db_connector.save_chat_history(chatroom_id, 'user', question)
                db_connector.save_chat_history(chatroom_id, 'llama', llama_response)
                response = {'response': llama_response, 'chatroom_id': chatroom_id}

            return jsonify(response)
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
            session_manager.delete_session(user_id, chatroom_id)
            return jsonify({'message': '채팅방 삭제 완료', 'chatroom_id': chatroom_id})
        finally:
            db_connector.close()
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
        return jsonify({'chat_history': chat_history})
    finally:
        db_connector.close()


def init_chat_module(app):
    app.register_blueprint(chat_bp)
    if 'OPENAI_API_KEY' not in app.config:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")