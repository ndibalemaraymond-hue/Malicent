#!/usr/bin/env python3
"""
Project Tool Demo

This script demonstrates the core functionality of the Project Tool platform.
"""

import asyncio
from project_tool.api import app

def main():
    import uvicorn
    print("Starting Project Tool...")
    print("API will be available at http://localhost:8000")
    print("Interactive API docs at http://localhost:8000/docs")
    print()
    print("Key endpoints:")
    print("- POST /auth/register - Register a new user")
    print("- POST /auth/token - Login and get access token")
    print("- POST /projects/ - Create a new project")
    print("- POST /ai/roadmap - Generate AI roadmap from proposal")
    print("- POST /finance/analyze - Run financial analysis")
    print("- POST /qr/generate - Generate QR access code")
    print()
    print("Example usage with curl:")
    print('curl -X POST "http://localhost:8000/auth/register" -H "Content-Type: application/json" -d \'{"username":"test","email":"test@example.com","password":"pass"}\'')
    print()
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
