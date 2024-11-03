---
# Project Setup

Follow these instructions to set up and run the backend and frontend of this project. Both servers should be run in separate terminal windows.
---

## Backend

Navigate to the `backend` directory in the root of the project:

```bash
cd backend
```

### 1. Set Up Python Virtual Environment (if required)

To create a virtual environment and activate it:

```bash
# Create a virtual environment
python3 -m venv venv

# Activate on Windows
venv\Scripts\activate.bat

# Activate on MacOS/Linux
source venv/bin/activate
```

If a virtual environment is not required, skip this step.

### 2. Install Python Packages

Install the required Python packages. If a `requirements.txt` file is present, run:

```bash
pip install -r requirements.txt
```

If `requirements.txt` is not provided, manually install the necessary packages and generate the file:

```bash
pip install flask flask-cors mysql-connector-python openpyxl tqdm
pip freeze > requirements.txt
```

### 3. Run the Flask API

To start the Flask server, run:

```bash
python server.py
```

The backend server will be accessible at [http://127.0.0.1:5000](http://127.0.0.1:5000).

---

## Frontend

Open a new terminal window, then navigate to the `frontend` directory in the root of the project:

```bash
cd frontend
```

### 1. Install NPM Packages

Install the required dependencies:

```bash
npm install
```

### 2. Run the Next.js Development Server

Start the development server with:

```bash
npm run dev
```

The frontend server will be accessible at [http://127.0.0.1:3000](http://127.0.0.1:3000).

---

**Note**: The backend and frontend should be run in separate terminals to function correctly.
