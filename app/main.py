import os
import threading
from flask import Flask

# Ajuste de sys.path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.routes import main_bp
from app.kafka_services import kafka_listener

# Forzar ruta absoluta para los templates
template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'templates'))
app = Flask(__name__, template_folder=template_dir)

# Evitar caché en navegadores
@app.after_request
def add_no_cache(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# Registrar endpoints
app.register_blueprint(main_bp)

if __name__ == '__main__':
    thread = threading.Thread(target=kafka_listener, daemon=True)
    thread.start()
    print("Iniciando Dashboard SISCO en http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
