
# LLM

- 자체 Build 모델 : 
  - https://huggingface.co/MLP-KTLim/llama-3-Korean-Bllossom-8B
- API: OpenAI GPT 모델 활용

- 현재 serving model 부분과 질문처리부분으로 flask 2개
- question -> llama3 -> 답변 or 부족한답변일경우 -> gpt  >> 로직 더 고민해봐야함. 부족한답변인지 판단..?
- OpenAI API -> langchain 모듈활용
- "http://112.217.124.195:30001/ask" postman 활용해서 json형태의 { "question" : "질문" } 동작 확인
-

- pip install torch transformers==4.40.0 accelerate
- pip install langchain openai langchain_community