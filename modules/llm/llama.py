import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from transformers import BitsAndBytesConfig
from functools import lru_cache
from flask import current_app


quantization_config = BitsAndBytesConfig(load_in_4bit=True)


class LlamaModel:
    _instance = None

    @classmethod
    def get_instance(cls, device=None):
        if cls._instance is None:
            cls._instance = cls(device)
        return cls._instance

    def __init__(self, device=None):
        if device is None:
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.device = device
        self.tokenizer = None
        self.model = None

    @lru_cache(maxsize=3)
    def load_model(self):
        if self.tokenizer is None or self.model is None:
            model_id = current_app.config['MODEL_ID']
            self.tokenizer = AutoTokenizer.from_pretrained(model_id)
            self.model = AutoModelForCausalLM.from_pretrained(
                model_id,
                quantization_config=quantization_config,
                torch_dtype=torch.bfloat16).to(self.device)
        return self.model, self.tokenizer

    def generate_response(self, instruction):
        model, tokenizer = self.load_model()
        PROMPT = '''
        You are a helpful AI assistant. 
        Please answer the user's questions kindly. 
        당신은 유능한 AI 어시스턴트 입니다. 사용자의 질문에 대해 친절하게 답변해주세요.
        답변내용이 부족하면 "gpt help"라고 하세요. 정보가 부족하여 답변하지못하면 "gpt help"라고 하세요.
        또한, 모든 대답을 JSON 형식으로 해주세요. 당신의 답변은 "answer" 키에 담아서 보내주세요. 또한, 사용자의 투자 유형에 대한 정보는 "user_invest_type" 키에 담아 보내주세요.
        '''
        messages = [
            {"role": "system", "content": f"{PROMPT}"},
            {"role": "user", "content": f"{instruction}"}
        ]
        input_ids = tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(self.device)
        outputs = model.generate(input_ids, max_new_tokens=2048, eos_token_id=tokenizer.eos_token_id, do_sample=True, temperature=0.6, top_p=0.9)
        response = tokenizer.decode(outputs[0][input_ids.shape[-1]:], skip_special_tokens=True)
        return response