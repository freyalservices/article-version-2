import os
import docx2txt
import PyPDF2
import spacy
import snowflake.connector

# Connect to Snowflake
ctx = snowflake.connector.connect(
    user='GURUDEVELOPMENT',
    password='Snowflake@guru7',
    account='vhhlgce-td31826',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN',
    database='ARTICLE',
    schema='ARTICLE'
)

cs = ctx.cursor()

# Snowflake stage name
stage_name = 'ARTICLE'

# Load the spaCy English language model
nlp = spacy.load("en_core_web_sm")

def extract_info_from_text(text):
    title = ""
    author = ""
    content = ""

    lines = text.strip().split('\n')
    if len(lines) >= 2:
        title = lines[0].strip()
        author_match = lines[1].strip().lower()
        if author_match.startswith("by "):
            author = author_match[3:].strip()
    
    if title:
        text = '\n'.join(lines[2:])
    else:
        text = '\n'.join(lines)
    
    return title, author, text

def extract_info_from_docx(file_path):
    text = docx2txt.process(file_path)
    return extract_info_from_text(text)

def extract_info_from_pdf(file_path):
    with open(file_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        text = ''
        for page in reader.pages:
            text += page.extract_text()
        return extract_info_from_text(text)

def extract_info_from_txt(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        text = file.read()
        return extract_info_from_text(text)

def extract_info_from_file(file_path):
    if file_path.endswith('.docx'):
        return extract_info_from_docx(file_path)
    elif file_path.endswith('.pdf'):
        return extract_info_from_pdf(file_path)
    elif file_path.endswith('.txt'):
        return extract_info_from_txt(file_path)
    else:
        raise ValueError("Unsupported file format")

def main():
    folder_path = 'files'  # Path to the folder containing files
    next_article_id = 1  # Starting article_id
    
    if os.path.exists(folder_path):
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                title, author, content = extract_info_from_file(file_path)
                print("File:", file_name)
                print("-" * 40)
                
                # Generate article_id and increment for next iteration
                article_id = next_article_id
                next_article_id += 1
                
                # Insert data into Snowflake table using parameterized queries
                insert_query = "INSERT INTO ARTICLE (ARTICLE_ID, TITLE, DESCRIPTION, FILE_URL) VALUES (%s, %s, %s, %s)"
                cs.execute(insert_query, (article_id, title, author, content))
                
                # Put the file into Snowflake stage
                stage_file_path = f'@{stage_name}/{file_name}'
                cs.execute(f"PUT file://{file_path} {stage_file_path}")
                
                # Update FILE_URL column with the stage file path
                update_query = "UPDATE ARTICLE SET FILE_URL = %s WHERE ARTICLE_ID = %s"
                cs.execute(update_query, (stage_file_path, article_id))
                
                # Commit the transaction
                ctx.commit()
                print('processed.')
                print("-" * 40)
        
        # Close the cursor and connection
        cs.close()
        ctx.close()
    else:
        print("Folder not found.")

if __name__ == "__main__":
    main()
