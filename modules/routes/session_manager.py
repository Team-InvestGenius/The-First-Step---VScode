import threading
import logging
from modules.db.file_storage import load_model_state, save_model_state, delete_model_state
from modules.llm.llama import LlamaModel


# 로그 설정
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 단일 모델 인스턴스를 전역 변수로 사용
llama_model = None

def initialize_model(model):
    global llama_model
    llama_model = model
    logger.debug("LlamaModel initialized.")

def get_or_create_model(user_id, chatroom_id):
    global llama_model
    if llama_model is None:  # 글로벌 모델이 초기화되지 않은 경우에만 로드하거나 생성
        model = load_model_state(user_id, chatroom_id)
        if model is not None:
            llama_model = model
        else:
            llama_model = LlamaModel()
            save_model_state(user_id, chatroom_id, llama_model)
    return llama_model

def async_save_model_state(user_id, chatroom_id, model):
    threading.Thread(target=save_model_state, args=(user_id, chatroom_id, model)).start()

def end_session(user_id, chatroom_id):
    global llama_model
    logger.debug(f"Ending session for user_id: {user_id}, chatroom_id: {chatroom_id}")
    if llama_model:
        try:
            logger.debug("Attempting to save model state")
            async_save_model_state(user_id, chatroom_id, llama_model)  # 비동기 저장 호출
            logger.debug("Model state save initiated")
        except Exception as e:
            logger.error(f"Error saving model state: {str(e)}")

def delete_session(user_id, chatroom_id):
    logger.debug(f"Deleting session for user_id: {user_id}, chatroom_id: {chatroom_id}")
    try:
        delete_model_state(user_id, chatroom_id)
        logger.debug("Model state deleted.")
    except Exception as e:
        logger.error(f"Error deleting model state: {str(e)}")
