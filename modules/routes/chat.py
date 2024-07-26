import ast
from flask import Blueprint, request, jsonify, current_app
from modules.db.chat_db import ChatDBConnector
from modules.routes.session_manager import SessionManager
from modules.llm.chat_gpt import GPTModel
from modules.llm.utils import format_recent_chat_history

chat_bp = Blueprint("chat", __name__)

# 키워드를 변수로 저장
KEYWORDS = ["죄송합니다", "모르겠습니다", "잘 모르겠", "gpt help"]

session_manager = SessionManager.get_instance()


def get_gpt_model():
    api_key = current_app.config["OPENAI_API_KEY"]
    model_id = current_app.config["GPT_MODEL_ID"]
    return GPTModel(api_key=api_key, model_id=model_id)


def evaluate_response(response: str, keywords: list) -> bool:
    """응답에 특정 키워드가 포함되어 있는지 확인"""
    return any(keyword in response for keyword in keywords)


@chat_bp.route("/ask", methods=["POST"])
def ask_question():
    try:
        data = request.get_json()
        question = data.get("question")
        user_id = data.get("user_id")
        room_id = data.get("room_id")

        if not all([question, user_id, room_id]):
            missing = []
            if not question: missing.append("질문")
            if not user_id: missing.append("유저 ID")
            if not room_id: missing.append("Room ID")
            return jsonify({"error": f"다음 정보가 제공되지 않았습니다: {', '.join(missing)}"}), 400

        db_connector = ChatDBConnector()
        chat_history = db_connector.get_chat_history(room_id)

        # Llama model response
        model = session_manager.get_model()
        formatted_chat_history = "\n".join(
            [f"{entry['speaker']}: {entry['message']}" for entry in chat_history]
        )
        model_response = model.generate_response(
            f"{formatted_chat_history}\nUser: {question}"
        )
        print(model_response)
        try:
            if isinstance(model_response, str):
                print(model_response)
                model_response = ast.literal_eval(model_response)
        except (ValueError, SyntaxError) as e:
            return jsonify({"error": f"대답을 변환하는 작업 중에 에러가 발생했습니다. {e}"}), 500

        model_answer = model_response.get('answer')
        user_invest_type = model_response.get('user_invest_type')
        answer_confidence = model_response.get('confidence')

        use_gpt = evaluate_response(model_answer, KEYWORDS) or answer_confidence < 0.4

        if use_gpt:
            gpt_model = get_gpt_model()
            formatted_chat_history = format_recent_chat_history(chat_history, n=3)
            gpt_response = gpt_model.generate_with_history(formatted_chat_history)

            if isinstance(gpt_response, str):
                gpt_response = ast.literal_eval(gpt_response)
            else:
                return jsonify({"error": "대답을 변환하는 작업 중에 에러가 발생했습니다."}), 500

            model_answer = gpt_response.get('answer', model_answer)
            user_invest_type = gpt_response.get('user_invest_type', user_invest_type)
            answer_confidence = gpt_response.get('confidence', answer_confidence)

        db_connector.save_chat_history(room_id, "user", question)
        db_connector.save_chat_history(room_id, "gpt" if use_gpt else "llama", model_answer)

        response = {
            "response": model_answer,
            "chatroom_id": room_id,
            "user_invest_type": user_invest_type,
            "confidence": answer_confidence
        }
        return jsonify(response)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db_connector.close()


@chat_bp.route("/delete-chatroom", methods=["POST"])
def delete_chatroom():
    try:
        data = request.get_json()
        chatroom_id = data.get("chatroom_id")
        user_id = data.get("user_id")

        if not chatroom_id or not user_id:
            return jsonify({"error": "채팅방 ID와 유저 ID가 제공되지 않았습니다."}), 400

        db_connector = ChatDBConnector()
        try:
            db_connector.delete_chatroom(chatroom_id)
            session_manager.delete_session(user_id, chatroom_id)
            return jsonify({"message": "채팅방 삭제 완료", "chatroom_id": chatroom_id})
        finally:
            db_connector.close()
    except Exception as e:
        return jsonify({"error": f"채팅방 삭제 중 오류 발생: {str(e)}"}), 500


@chat_bp.route("/history", methods=["GET"])
def get_history():
    chatroom_id = request.args.get("chatroom_id")
    if not chatroom_id:
        return jsonify({"error": "채팅방 ID가 제공되지 않았습니다."}), 400

    db_connector = ChatDBConnector()
    try:
        chat_history = db_connector.get_chat_history(chatroom_id)
        return jsonify({"chat_history": chat_history})
    finally:
        db_connector.close()


@chat_bp.route("/create-room", methods=["POST"])
def create_room():
    try:
        data = request.get_json()
        user_id = data.get("user_id")

        if not user_id:
            return jsonify({"error": "user_id 제공되지 않음"}), 400

        db_connector = ChatDBConnector()
        try:
            chatroom_count = db_connector.get_chatroom_count_by_userid(user_id)
            if chatroom_count >= 3:
                return (
                    jsonify({"error": "최대 3개의 채팅방만 생성할 수 있습니다."}),
                    400,
                )
            room_id = db_connector.create_chatroom(user_id)
            if not room_id:
                raise Exception("Chat room creation failed")

            return jsonify({"room_id": room_id})
        finally:
            db_connector.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def init_chat_module(app):
    app.register_blueprint(chat_bp)
    if "OPENAI_API_KEY" not in app.config:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")
