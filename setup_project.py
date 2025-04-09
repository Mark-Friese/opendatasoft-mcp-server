"""
Setup script to create the correct directory structure and files for the Opendatasoft MCP Server.
"""
import os
import shutil

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def create_file(path, content=""):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Created file: {path}")

def main():
    # Create main directories
    create_directory("tools")
    
    # Copy/move files to the correct locations
    files_to_create = [
        ("__init__.py", ""),
        ("tools/__init__.py", ""),
        ("ods_api.py", "# Move your ods_api.py content here"),
        ("tools/catalog_tools.py", "# Move your catalog_tools.py content here"),
        ("tools/query_tools.py", "# Move your query_tools.py content here"),
        ("tools/analysis_tools.py", "# Move your analysis_tools.py content here"),
        ("main.py", "# Move your main.py content here"),
        ("requirements.txt", "httpx>=0.25.0\nmcp>=1.2.0")
    ]
    
    for file_path, content in files_to_create:
        create_file(file_path, content)
    
    print("\nProject structure created successfully!")
    print("Next steps:")
    print("1. Copy the content of your actual implementation files into the created files")
    print("2. Run 'pip install -r requirements.txt' to install dependencies")
    print("3. Start the MCP server with 'python main.py'")

if __name__ == "__main__":
    main()