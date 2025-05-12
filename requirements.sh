#!/bin/bash

# Determine the correct Python 3.11 command based on the operating system
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  PYTHON_CMD="py -3.11"
else
  if ! command -v python3.11 &>/dev/null; then
    echo "Python 3.11 not found. Please install it first."
    exit 1
  fi
  PYTHON_CMD="python3.11"
fi

# Check the detected Python version
PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
if [[ "$PYTHON_VERSION" != "3.11" ]]; then
  echo "Detected Python version is $PYTHON_VERSION. Python 3.11 is required."
  exit 1
fi

# Create the virtual environment if it does not exist
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment using $PYTHON_CMD..."
  $PYTHON_CMD -m venv .venv || { echo "Error creating virtual environment"; exit 1; }
else
  echo "Virtual environment already exists."
fi

# Activate the virtual environment (Windows or Unix-based systems)
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  source .venv/Scripts/activate 2>/dev/null || { echo "Error activating virtual environment on Windows"; exit 1; }
else
  source .venv/bin/activate 2>/dev/null || { echo "Error activating virtual environment on Unix/Linux"; exit 1; }
fi

# Install dependencies from requirements.txt (excluding torch)
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt || { echo "Error installing requirements.txt"; exit 1; }

# Install torch with CUDA 11.8
echo "Installing torch with CUDA 11.8..."
pip install --force-reinstall torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118 || { echo "Error installing torch"; exit 1; }

# Success message
echo "Setup complete: virtual environment created and all dependencies installed."
