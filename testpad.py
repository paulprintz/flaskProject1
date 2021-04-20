from waitress import serve
import app
if __name__ == '__main__':
    # app.run(debug=True)
    print("stat server")
    serve(app.app, host='0.0.0.0', port=5001)