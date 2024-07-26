import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from transformers import BitsAndBytesConfig
from functools import lru_cache
from flask import current_app
from modules.llm.utils import PROMPT


def clear_cuda_cache():
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


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

    @lru_cache(maxsize=1)
    def load_model(self):
        if self.tokenizer is None or self.model is None:
            model_id = current_app.config['MODEL_ID']
            model_path = current_app.config["MODEL_PATH"]
            self.tokenizer = AutoTokenizer.from_pretrained(model_id)
            self.model = AutoModelForCausalLM.from_pretrained(
                model_id,
                torch_dtype=torch.bfloat16,
                device_map="auto"
            )
        return self.model, self.tokenizer

    def generate_response(self, instruction):
        model, tokenizer = self.load_model()
        messages = [
            {"role": "system", "content": f"{PROMPT}"},
            {"role": "user", "content": f"{instruction}"}
        ]
        input_ids = tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(self.device)
        outputs = model.generate(input_ids, max_new_tokens=2048, eos_token_id=tokenizer.eos_token_id, do_sample=True, temperature=0.6, top_p=0.9)
        response = tokenizer.decode(outputs[0][input_ids.shape[-1]:], skip_special_tokens=True)

        # CUDA 캐시 정리
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        return response
