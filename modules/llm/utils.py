import json
from datetime import datetime


LLAMA_PROMPT = """
입력받은 언어를 인식하여, 해당 언어로 대답 해주세요. 모든 질문에 대해 JSON 형식으로만 응답해주세요. 응답은 다음 구조를 따라야 합니다:

{
  "answer": "귀하의 답변",
  "confidence": 0에서 1 사이의 숫자 (확신도),
  "user_invest_type": "귀하가 생각하는 사용자의 투자 유형"
}

"user_invest_type" 에는 대화 중에 사용자의 투자 유형에 대한 응답을 할 경우에는 '공격투자형', '중립투자형', '방어투자형' 의 3가지 중의 하나의 값을 'user_invest_type' 키에 담아 보내주세요.
또한, 당신의 답변이 얼마나 정확한지에 대한 확신도를 'confidence' 키에 담아 보내주세요.
또한, 당신의 대답에 'JSON 형식으로 응답합니다' 라는 말은 하지 말아주세요. 오로지 JSON 형식으로만 대답을 해야 합니다. 

만약 응답이 이 JSON 형식을 따르지 않을 경우, 다음과 같은 오류 메시지를 반환하세요:

{
  "error": true,
  "message": "응답이 올바른 JSON 형식이 아닙니다.",
  "expected_format": {
    "answer": "string",
    "confidence": "number (0-1)",
    "additional_info": "string (optional)"
  }
}

위의 지침을 따라 응답해주세요.
"""

GPT_PROMPT = """
입력받은 언어를 인식하여, 해당 언어로 대답 해주세요. 모든 질문에 대해 JSON 형식으로만 응답해주세요. 응답은 다음 구조를 따라야 합니다:

{
  "answer": "귀하의 답변",
  "confidence": 0에서 1 사이의 숫자 (확신도),
  "user_invest_type": "귀하가 생각하는 사용자의 투자 유형"
}

"user_invest_type" 에는 대화 중에 사용자의 투자 유형에 대한 응답을 할 경우에는 '공격투자형', '중립투자형', '방어투자형' 의 3가지 중의 하나의 값을 'user_invest_type' 키에 담아 보내주세요.
또한, 당신의 답변이 얼마나 정확한지에 대한 확신도를 'confidence' 키에 담아 보내주세요.
또한, 당신의 대답에 'JSON 형식으로 응답합니다' 라는 말은 하지 말아주세요. 오로지 JSON 형식으로만 대답을 해야 합니다. 

만약 응답이 이 JSON 형식을 따르지 않을 경우, 다음과 같은 오류 메시지를 반환하세요:

{
  "error": true,
  "message": "응답이 올바른 JSON 형식이 아닙니다.",
  "expected_format": {
    "answer": "string",
    "confidence": "number (0-1)",
    "additional_info": "string (optional)"
  }
}

위의 지침을 따라 응답해주세요.
"""


def format_recent_chat_history(chat_history, n=5):
    # 시간순으로 정렬 (타임스탬프가 문자열인 경우를 대비해 datetime 객체로 변환)
    sorted_history = sorted(
        chat_history,
        key=lambda x: (
            datetime.fromisoformat(x["timestamp"])
            if isinstance(x["timestamp"], str)
            else x["timestamp"]
        ),
    )

    # 최근 N개 선택
    recent_history = sorted_history[-n:]

    # ChatGPT API 형식으로 변환
    formatted_messages = []
    for entry in recent_history:
        if entry["speaker"] == "user":
            role = "user"
        elif entry["speaker"] in ["llama", "gpt"]:
            role = "assistant"
        else:
            print(f"Warning: Unknown speaker type: {entry['speaker']}")
            continue  # 알 수 없는 speaker는 무시

        formatted_messages.append({"role": role, "content": entry["message"]})

    return formatted_messages
