import os.path
import save_data
from flask import Flask, render_template, request
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'D:/Pycharm/NetworkX/input_data'

@app.route('/')
def home_page():
    return render_template('upload.html')

@app.route('/fileUpload', methods = ['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        f.save(os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(f.filename)))
        save_data.db_create()
        save_data.csv_create()
        return f'SAVE COMPLETE - {secure_filename(f.filename)} :)'

if __name__ == '__main__':
    app.run(debug=True)