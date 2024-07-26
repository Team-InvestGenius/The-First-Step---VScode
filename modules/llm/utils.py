import json
from datetime import datetime


LLAMA_PROMPT = """
You are a helpful AI assistant. Please answer the user's questions kindly. 
당신은 유능한 AI 어시스턴트 입니다. 사용자의 질문에 대해 친절하게 답변해주세요. 모든 대답은 Json 형식으로 답변 합니다. Json의 Key는 'answer', 'user_invest_type', 'confidence' 입니다. 당신의 답변은 'answer' 키에 담아서 보내주세요. 
만약 대화 중에 사용자의 투자 유형에 대한 응답을 할 경우에는 '공격투자형', '중립투자형', '방어투자형' 의 3가지 중의 하나의 값을 'user_invest_type' 키에 담아 보내주세요. 또한, 당신의 답변이 얼마나 정확한지에 대한 확신도를 'confidence' 키에 담아 보내주세요. 당신이 대답하다가 잘 모르겠거나, 답변 내용이 부족한 경우라면 'answer' 에 'gpt help' 라고 답변 해주세요.
"""


GPT_PROMPT = """
당신은 유능한 AI 어시스턴트 입니다. 사용자의 질문에 대해 친절하게 답변 해주세요. 입력받은 언어를 인식하여, 해당 언어로 대답 해주세요. 한국어가 입력 되었다면 한국어로 대답 해야 합니다.  모든 대답은 Json 형식으로 답변 합니다. Json의 Key는 'answer', 'user_invest_type', 'confidence' 입니다. 당신의 답변은 'answer' 키에 담아서 보내주세요. 
만약 대화 중에 사용자의 투자 유형에 대한 응답을 할 경우에는 '공격투자형', '중립투자형', '방어투자형' 의 3가지 중의 하나의 값을 'user_invest_type' 키에 담아 보내주세요. 또한, 당신의 답변이 얼마나 정확한지에 대한 확신도를 'confidence' 키에 담아 보내주세요. 
"""


def format_recent_chat_history(chat_history, n=5):
    # 시간순으로 정렬 (타임스탬프가 문자열인 경우를 대비해 datetime 객체로 변환)
    sorted_history = sorted(chat_history, key=lambda x: datetime.fromisoformat(x['timestamp']) if isinstance(x['timestamp'], str) else x['timestamp'])

    # 최근 N개 선택
    recent_history = sorted_history[-n:]

    # ChatGPT API 형식으로 변환
    formatted_messages = []
    for entry in recent_history:
        if entry['speaker'] == 'user':
            role = 'user'
            content = entry['message']
        elif entry['speaker'] in ['llama', 'gpt']:
            role = 'assistant'
            # JSON 형식의 응답에서 'answer' 필드 추출
            try:
                content_dict = json.loads(entry['message'])
                content = content_dict.get('answer', entry['message'])
            except json.JSONDecodeError:
                content = entry['message']
                print(f"Warning: Failed to parse JSON for message: {entry['message'][:50]}...")
        else:
            print(f"Warning: Unknown speaker type: {entry['speaker']}")
            continue  # 알 수 없는 speaker는 무시

        formatted_messages.append({
            "role": role,
            "content": content
        })

    return formatted_messages