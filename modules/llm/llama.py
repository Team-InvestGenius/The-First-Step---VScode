import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

class LlamaModel:
    def __init__(self, model_id='MLP-KTLim/llama-3-Korean-Bllossom-8B', device=None):
        if device is None:
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.device = device
        self.tokenizer = AutoTokenizer.from_pretrained(model_id)
        self.model = AutoModelForCausalLM.from_pretrained(model_id, torch_dtype=torch.bfloat16).to(self.device)

    def generate_response(self, instruction):
        PROMPT = '''You are a helpful AI assistant. Please answer the user's questions kindly. 당신은 유능한 AI 어시스턴트 입니다. 사용자의 질문에 대해 친절하게 답변해주세요.
        답변내용이 부족하면 "gpt help"라고 하세요. 정보가 부족하여 답변하지못하면 "gpt help"라고 하세요'''
        messages = [
            {"role": "system", "content": f"{PROMPT}"},
            {"role": "user", "content": f"{instruction}"}
        ]
        input_ids = self.tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(self.device)
        outputs = self.model.generate(input_ids, max_new_tokens=2048, eos_token_id=self.tokenizer.eos_token_id, do_sample=True, temperature=0.6, top_p=0.9)
        response = self.tokenizer.decode(outputs[0][input_ids.shape[-1]:], skip_special_tokens=True)
        return response



