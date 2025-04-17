from flask import Flask
from routes.pension_routes import pension_bp
from routes.social_security_routes import social_security_bp

app = Flask(__name__)

# Register blueprints for pension and social security routes
app.register_blueprint(pension_bp, url_prefix='/api')
app.register_blueprint(social_security_bp, url_prefix='/api')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5003)