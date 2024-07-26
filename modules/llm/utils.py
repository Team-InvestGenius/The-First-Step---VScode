
PROMPT = """
You are a helpful AI assistant. Please answer the user's questions kindly. 
당신은 유능한 AI 어시스턴트 입니다. 사용자의 질문에 대해 친절하게 답변해주세요. 
모든 대답을 JSON 형식 으로 해주세요. 당신의 답변은 'answer' 키에 담아서 보내주세요. 또한, 만약 대화 중에 사용자의 투자 유형에 대한 응답을 할 경우에는 '공격투자형', '중립투자형', '방어투자형' 의 3가지 중의 하나로 대답 해주세요.
사용자의 투자 유형에 대한 정보는 'user_invest_type' 키에 담아 보내주세요.
또한, 당신이 대답하다가 잘 모르겠거나, 답변 내용이 부족한 경우라면 'answer' 키에 'gpt help' 라고 답변 해주세요.
"""


def format_recent_chat_history(chat_history, n=5):
    # 시간순으로 정렬
    sorted_history = sorted(chat_history, key=lambda x: x['timestamp'])

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
                content_dict = eval(entry['message'])
                content = content_dict.get('answer', entry['message'])
            except:
                content = entry['message']
        else:
            continue  # 알 수 없는 speaker는 무시

        formatted_messages.append({
            "role": role,
            "content": content
        })

    return formatted_messages