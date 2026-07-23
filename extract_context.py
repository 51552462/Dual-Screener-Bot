import os

# 무시할 폴더 및 파일 확장자 세팅
IGNORE_DIRS = {'.git', '__pycache__', 'venv', 'env', '.idea', '.vscode', 'data', 'logs'}
ALLOWED_EXTENSIONS = {'.py', '.md', '.json'}
OUTPUT_FILE = 'quant_blueprint.txt'

def generate_blueprint():
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        outfile.write("=== QUANT FACTORY PROJECT BLUEPRINT ===\n\n")
        
        # 1. 디렉토리 트리 구조 먼저 작성
        outfile.write("## 1. Directory Tree\n")
        for root, dirs, files in os.walk('.'):
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            level = root.replace('.', '').count(os.sep)
            indent = ' ' * 4 * (level)
            outfile.write(f"{indent}{os.path.basename(root)}/\n")
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                if any(f.endswith(ext) for ext in ALLOWED_EXTENSIONS):
                    outfile.write(f"{subindent}{f}\n")
        
        outfile.write("\n\n## 2. File Contents\n")
        
        # 2. 핵심 파일 내용 병합
        for root, dirs, files in os.walk('.'):
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            for file in files:
                if any(file.endswith(ext) for ext in ALLOWED_EXTENSIONS) and file != 'extract_context.py':
                    file_path = os.path.join(root, file)
                    outfile.write(f"\n\n{'='*50}\n")
                    outfile.write(f"FILE: {file_path}\n")
                    outfile.write(f"{'='*50}\n")
                    try:
                        with open(file_path, 'r', encoding='utf-8') as infile:
                            outfile.write(infile.read())
                    except Exception as e:
                        outfile.write(f"Error reading file: {e}\n")

    print(f"완료! '{OUTPUT_FILE}' 파일이 생성되었습니다. 이 파일을 통째로 AI에게 업로드하세요.")

if __name__ == "__main__":
    generate_blueprint()