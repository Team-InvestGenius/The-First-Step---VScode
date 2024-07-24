import os
from flask import Flask
from flask_cors import CORS
from modules.routes.chat import chat_bp

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.register_blueprint(chat_bp)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=30000, debug=False)
