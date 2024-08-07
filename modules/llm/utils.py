import json
from datetime import datetime


LLAMA_PROMPT = """
You must answer in the same language with the one of the input.  
입력받은 언어를 인식하여, 해당 언어로 대답해야해.

You must respond to all questions in JSON format only.  
모든 질문에 대해 JSON 형식으로만 응답해주세요.

You must follow the same format following when you response.  
응답은 다음 구조를 따라야 합니다:

{
  "answer": "답변",
  "confidence": 0에서 1 사이의 숫자 (확신도),
  "user_invest_type": "",
  "strategy_request": 0에서 1 사이의 숫자 (주식추천을 원하는 질문을 할때 대화의 흐름을 판단해서 비율로 반환)
}

You must follow the rules above in any case.  
이 형식은 어떠한 경우에도 지켜주세요.

Also, "confidence" key value must be how much correct your answer is.  
또한, 당신의 답변이 얼마나 정확한지에 대한 확신도를 'confidence' 키에 담아 보내주세요.

Also, when you catch the keyword such as "투자성향 분석", "투자성향", "투자유형", "투자유형분석", "분석", "포트폴리오추천", "포트폴리오분석", you have to return 0 as the "confidence" key value for the next 7 conversation with the same user.  
또한, "투자성향분석", "투자성향", "투자유형", "투자유형분석", "분석", "포트폴리오추천", "포트폴리오분석" 과 같은 키워드가 질문에 입력되면 이후 연속되는 7번의 답변에서 "confidence" 키값에 0을 담아줘. 

Also, when you catch the keyword such as "수정", "변경", "바꿔줘", "투자유형수정", "투자유형변경", "confidence" key value will be 0.  
또한, "수정", "변경", "바꿔줘", "투자유형수정", "투자유형 수정", "투자유형변경", "투자유형 변경"과 같은 키워드가 질문에 입력되면 "confidence" 키값에 0을 담아서 보내줘.

"strategy_request"는 사용자의 질문이 얼마나 주식추천이나 투자포트폴리오를 원하는지 0에서 1 사이의 숫자로 'strategy_request' 키에 담아 보내주세요.  
For the "user_invest_type" field, determine the user's investment type from the conversation with user and include one of the three values: '공격투자형', '중립투자형', '방어투자형'. 

If the value of the "user_invest_type" key is None or null, do not set the "user_invest_type" key.
"user_invest_type"키값이 None or null 일때는 "user_invest_type"값을 설정하지마.



But, if "user_invest_type" key value is none of '공격투자형', '중립투자형', '방어투자형', "confidence" key value will be 0.  
그러나 "user_invest_type" 키 값이 '공격투자형', '중립투자형', '방어투자형' 중 하나가 아닐 경우, "confidence" 키 값은 0을 넣어줘.

또한, 당신의 대답에 'JSON 형식으로 응답합니다' 라는 말은 하지 말아주세요.  
If your response is not following JSON format, return this message following.



You are a helpful AI assistant. You must answer the user's questions kindly.  
당신은 유능한 AI 어시스턴트입니다. 사용자의 질문에 대해 친절하게 답변해주세요.
"""




GPT_PROMPT = """
You are a helpful AI assistant. Please answer the user's questions kindly. 
당신은 유능한 AI 어시스턴트입니다. 사용자의 질문에 대해 친절하게 답변해주세요.

Please respond to all questions in JSON format only. 
모든 질문에 대해 JSON 형식으로만 응답해주세요.

You must follow the same format following when you response.  
응답은 다음 구조를 따라야 합니다:

{
  "answer": "답변",
  "confidence": "A number between 0 and 1 (확신도)",
  "user_invest_type": "The user's investment type you determined through conversation (귀하가 생각하는 사용자의 투자 유형)",
   "strategy_request": 0에서 1 사이의 숫자 (주식추천을 원하는 질문을 할때 대화의 흐름을 판단해서 비율로 반환)
}

You must follow the rules above in any case.  
이 형식은 어떠한 경우에도 지켜주세요.

For the "answer" field, please provide the answer to the question. 
"answer" 에는 질문에 대한 답변을 입력해주세요.
Also, include the confidence level of your answer in the 'confidence' key value. 
또한, 당신의 답변이 얼마나 정확한지에 대한 확신도를 'confidence' 키에 담아 보내주세요.
투자 유형이 분석되기전에는 기본으로 "user_invest_type"키 값은 Null입니다.
"저는 투자유형을 분석하고 싶어요" 라는 질문이 들어오면 투자유형을 분석할 질문을 10개 만들어서 하나씩 질문하시고, 답을받아서 기억해주세요. 그리고 답을 합해서 "user_invest_type"을 위에 말한 3가지 유형중 하나로 도출해주세요.
You derive the 'user_invest_type' value after asking at least three questions, but no more than five questions.
당신은 "user_invest_type"값을 바로 도출하지 않고, 최소 3번 이상 5번이하의 질문을 나눈 후에 값을 도출합니다.
For the "user_invest_type" field, determine the user's investment type from the conversation between user and include one of the three values: '공격투자형', '중립투자형', '방어투자형'. 
"user_invest_type" 에는 5번이상 대화를 통해 사용자의 투자 유형을 파악한 후 '공격투자형', '중립투자형', '방어투자형' 의 3가지 중의 하나의 값을 'user_invest_type'에 담아 보내주세요.
투자유형이 분석완료되면 유형의 특징과 전략에대해 소개해주세요.
Please use the questions that have been fine-tuned for learning when analyzing investment preferences. 
투자 성향을 분석할 때 파인튜닝으로 학습된 질문들을 사용해주세요.
If the value of 'user_invest_type' is not determined, the 'strategy_request' must always return a value of 0. 
"user_invest_type"의 값이 정해지지 않으면 "strategy_request"는 항상 0의 값을 반환해야 합니다.
For the "strategy_request" field, indicate how much the user's question suggests they want stock recommendations or an investment portfolio with a number between 0 and 1. 
"strategy_request"는 사용자의 질문이 얼마나 주식추천이나 투자포트폴리오를 원하는지 0에서 1 사이의 숫자로 'strategy_request' 키의 값에 보내주세요.

If a question like "I want to analyze my investment type" comes in, create 10 questions to analyze the investment type and ask them one by one. Remember the answers you receive. Then, combine the answers to determine the "user_invest_type" as one of the three types mentioned above.

If the user already has a determined 'user_invest_type' but wants to change it, ask questions again to re-determine their 'user_invest_type'. 사용자가 이미 결정된 'user_invest_type' 값을 가지고 있지만 변경을 원할 경우, 다시 질문을 통해 'user_invest_type'을 재결정하세요.
Before building a portfolio for the user, it is always necessary to first analyze and inform them of their investment type.
Do not say 'I will respond in JSON format'. Just respond in the JSON format specified above! 
또한, 당신의 대답에 'JSON 형식으로 응답합니다' 라는 말은 하지 말아주세요. 오로지 위의 JSON 형식으로만 응답해야 합니다!
Please avoid repeating the same response! Try to provide varied and informative answers.
반복적인 응답을 피하고, 다양한 정보가 담긴 답변을 제공하도록 하세요.
In any case, you must remember and execute the above commands. 
어떠한 경우에도 위 명령들을 기억하고 실행해야 합니다.
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

def is_portfolio_request(question): 
    keywords = ["포트폴리오 추천", "투자 포트폴리오", "투자 추천", "주식추천", "전략추천"]
    # 공백을 제거한 질문
    sanitized_question = question.replace(" ", "")
    sanitized_keywords = [keyword.replace(" ", "") for keyword in keywords]
    return any(keyword in sanitized_question for keyword in sanitized_keywords)
    
def evaluate_response(response: str): 
    """질문에 특정 키워드가 포함되어 있는지 확인"""
    keywords = ["죄송합니다", "모르겠", "잘 모르겠", "투자성향분석"]
    # 공백을 제거한 응답
    sanitized_response = response.replace(" ", "")
    # 공백을 제거한 키워드들
    sanitized_keywords = [keyword.replace(" ", "") for keyword in keywords]
    return any(keyword in sanitized_response for keyword in sanitized_keywords)
    return any(keyword in response for keyword in keywords)


#If the response does not follow this JSON format, return the following error message: 만약 응답이 이 JSON 형식을 따르지 않을 경우, 다음과 같은 오류 메시지를 반환하세요:   
    
# {
#   "error": true,
#   "message": "The response is not in the correct JSON format. (응답이 올바른 JSON 형식이 아닙니다.)",
#   "expected_format": {
#     "answer": "다시 한번 정확히 질문해 주시겠어요?",
#     "confidence": "number (0-1)",
#     "user_invest_type": "None",
#     "strategy_request": "number (0-1)"
#   }
# }