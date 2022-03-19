from energy_app import app
from werkzeug.middleware.profiler import ProfilerMiddleware

if __name__ == "__main__":
  #app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[5])
  app.run(host='0.0.0.0', port=5001, debug=True)
